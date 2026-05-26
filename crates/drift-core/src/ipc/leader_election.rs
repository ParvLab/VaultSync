use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use crate::error::DriftError;

use crate::storage::traits::StorageConfig;
use sha2::{Sha256, Digest};

#[cfg(windows)]
type LockHandle = windows_sys::Win32::Foundation::HANDLE;

#[cfg(unix)]
type LockHandle = std::fs::File;

#[cfg(not(any(windows, unix)))]
type LockHandle = ();

pub struct LeaderElection {
    namespace: String,
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

    pub fn try_acquire(&self) -> Result<bool, DriftError> {
        let mut handle_guard = self.lock_handle.lock().unwrap();
        if handle_guard.is_some() {
            self.is_leader.store(true, Ordering::SeqCst);
            return Ok(true);
        }

        #[cfg(windows)]
        {
            use windows_sys::Win32::System::Threading::CreateMutexW;
            use windows_sys::Win32::Foundation::{GetLastError, CloseHandle, ERROR_ALREADY_EXISTS};
            
            let name = format!("Global\\drift-{}-{}", self.namespace, self.unique_key);
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
            let lock_path = std::env::temp_dir().join(format!("drift-{}-{}.lock", self.namespace, self.unique_key));
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&lock_path)
                .map_err(|e| DriftError::Storage(format!("failed to open lock file: {e}")))?;
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
                    Err(DriftError::Storage(format!("flock failed: {}", last_error)))
                }
            }
        }

        #[cfg(not(any(windows, unix)))]
        {
            self.is_leader.store(true, Ordering::SeqCst);
            *handle_guard = Some(());
            Ok(true)
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
            #[cfg(not(any(windows, unix)))]
            {
                drop(handle);
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

