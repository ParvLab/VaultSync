use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use crate::error::VaultSyncError;

use crate::storage::traits::StorageConfig;
use sha2::{Sha256, Digest};

#[cfg(windows)]
type LockHandle = windows_sys::Win32::Foundation::HANDLE;

#[cfg(unix)]
type LockHandle = std::fs::File;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(inline_js = r#"
export function acquire_web_lock(lock_name, on_acquired, on_released) {
    let release_promise = new Promise((resolve) => {
        window._vaultsync_releases = window._vaultsync_releases || {};
        window._vaultsync_releases[lock_name] = resolve;
    });
    navigator.locks.request(lock_name, async (lock) => {
        on_acquired();
        await release_promise;
        on_released();
    }).catch(err => {
        console.error("Web Lock acquisition failed", err);
    });
}
export function release_web_lock(lock_name) {
    if (window._vaultsync_releases && window._vaultsync_releases[lock_name]) {
        window._vaultsync_releases[lock_name]();
        delete window._vaultsync_releases[lock_name];
    }
}
"#)]
extern "C" {
    fn acquire_web_lock(lock_name: &str, on_acquired: &js_sys::Function, on_released: &js_sys::Function);
    fn release_web_lock(lock_name: &str);
}

#[cfg(target_arch = "wasm32")]
pub struct WasmLockState {
    name: String,
    _acquired: wasm_bindgen::closure::Closure<dyn FnMut()>,
    _released: wasm_bindgen::closure::Closure<dyn FnMut()>,
}

#[cfg(target_arch = "wasm32")]
type LockHandle = WasmLockState;

#[cfg(all(not(any(windows, unix)), not(target_arch = "wasm32")))]
type LockHandle = ();

pub struct LeaderElection {
    #[allow(dead_code)]
    namespace: String,
    #[allow(dead_code)]
    unique_key: String,
    is_leader: Arc<AtomicBool>,
    lock_handle: std::sync::Mutex<Option<LockHandle>>,
}

impl LeaderElection {
    pub fn new(namespace: &str, storage: &StorageConfig) -> Self {
        let unique_key = match storage {
            StorageConfig::Sqlite { path } => {
                let mut hasher = Sha256::new();
                hasher.update(path.as_bytes());
                hex::encode(hasher.finalize())
            }
            StorageConfig::InMemory => "in-memory".to_string(),
            StorageConfig::Wasm => "wasm".to_string(),
        };

        Self {
            namespace: namespace.to_string(),
            unique_key,
            is_leader: Arc::new(AtomicBool::new(false)),
            lock_handle: std::sync::Mutex::new(None),
        }
    }

    pub fn try_acquire(&self) -> Result<bool, VaultSyncError> {
        let mut handle_guard = self.lock_handle.lock().unwrap();
        
        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::JsCast;
            if handle_guard.is_some() {
                return Ok(self.is_leader.load(Ordering::SeqCst));
            }

            let is_leader_clone = self.is_leader.clone();
            let is_leader_clone2 = self.is_leader.clone();
            let name = format!("vaultsync-{}-{}", self.namespace, self.unique_key);
            let name_clone = name.clone();

            let on_acquired = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                is_leader_clone.store(true, Ordering::SeqCst);
                tracing::info!("[LeaderElection] Acquired lock for {}", name_clone);
            }) as Box<dyn FnMut()>);

            let name_clone2 = name.clone();
            let on_released = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                is_leader_clone2.store(false, Ordering::SeqCst);
                tracing::info!("[LeaderElection] Released lock for {}", name_clone2);
            }) as Box<dyn FnMut()>);

            acquire_web_lock(&name, on_acquired.as_ref().unchecked_ref(), on_released.as_ref().unchecked_ref());

            *handle_guard = Some(WasmLockState {
                name,
                _acquired: on_acquired,
                _released: on_released,
            });

            Ok(false)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            if handle_guard.is_some() {
                self.is_leader.store(true, Ordering::SeqCst);
                return Ok(true);
            }

            #[cfg(windows)]
            {
                use windows_sys::Win32::System::Threading::CreateMutexW;
                use windows_sys::Win32::Foundation::{GetLastError, CloseHandle, ERROR_ALREADY_EXISTS};
                
                let name = format!("Global\\vaultsync-{}-{}", self.namespace, self.unique_key);
                let mut name_u16: Vec<u16> = name.encode_utf16().collect();
                name_u16.push(0);

                let handle = unsafe { CreateMutexW(std::ptr::null(), 1, name_u16.as_ptr()) };
                if handle == 0 {
                    return Ok(false);
                }
                let last_err = unsafe { GetLastError() };
                if last_err == ERROR_ALREADY_EXISTS {
                    unsafe { CloseHandle(handle); }
                    return Ok(false);
                }
                *handle_guard = Some(handle);
                self.is_leader.store(true, Ordering::SeqCst);
                Ok(true)
            }

            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let lock_path = std::env::temp_dir().join(format!("vaultsync-{}-{}.lock", self.namespace, self.unique_key));
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&lock_path)
                    .map_err(|e| VaultSyncError::Storage(format!("failed to open lock file: {e}")))?;
                let fd = file.as_raw_fd();
                let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
                if ret == 0 {
                    *handle_guard = Some(file);
                    self.is_leader.store(true, Ordering::SeqCst);
                    Ok(true)
                } else {
                    let last_error = std::io::Error::last_os_error();
                    if last_error.kind() == std::io::ErrorKind::WouldBlock || last_error.raw_os_error() == Some(libc::EWOULDBLOCK) || last_error.raw_os_error() == Some(libc::EAGAIN) {
                        Ok(false)
                    } else {
                        Err(VaultSyncError::Storage(format!("flock failed: {}", last_error)))
                    }
                }
            }

            #[cfg(all(not(any(windows, unix)), not(target_arch = "wasm32")))]
            {
                self.is_leader.store(true, Ordering::SeqCst);
                *handle_guard = Some(());
                Ok(true)
            }
        }
    }

    pub fn release(&self) {
        let mut handle_guard = self.lock_handle.lock().unwrap();
        if let Some(handle) = handle_guard.take() {
            #[cfg(windows)]
            {
                use windows_sys::Win32::System::Threading::ReleaseMutex;
                use windows_sys::Win32::Foundation::CloseHandle;
                unsafe {
                    ReleaseMutex(handle);
                    CloseHandle(handle);
                }
            }
            #[cfg(unix)]
            {
                drop(handle);
            }
            #[cfg(target_arch = "wasm32")]
            {
                release_web_lock(&handle.name);
            }
            #[cfg(all(not(any(windows, unix)), not(target_arch = "wasm32")))]
            {
                let _ = handle;
            }
        }
        self.is_leader.store(false, Ordering::SeqCst);
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }
}

impl Drop for LeaderElection {
    fn drop(&mut self) {
        self.release();
    }
}

