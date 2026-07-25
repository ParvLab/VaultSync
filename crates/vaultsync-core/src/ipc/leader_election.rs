use crate::error::VaultSyncError;
use crate::runtime_bus::{LeaderEvent, RuntimeBus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::storage::traits::StorageConfig;
use sha2::{Digest, Sha256};

// ── LockHandle type alias (platform-specific) ──

#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
pub struct WasmLockState {
    name: String,
    id: String,
}

#[cfg(windows)]
type LockHandle = windows_sys::Win32::Foundation::HANDLE;

#[cfg(unix)]
type LockHandle = std::fs::File;

#[cfg(target_arch = "wasm32")]
type LockHandle = WasmLockState;

#[cfg(all(not(any(windows, unix)), not(target_arch = "wasm32")))]
type LockHandle = ();

// ── AcquireMode ──

/// How to attempt leader lock acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireMode {
    /// Use `{ifAvailable: true}` — returns immediately with lock or waiting status.
    /// Lock is held if acquired. Suitable for first check at startup.
    Immediate,
    /// Issue a blocking request — returns Waiting immediately.
    /// When the lock becomes available, LeaderEvent::Acquired is emitted on the bus.
    /// Suitable for followers waiting for promotion.
    Wait,
}

// ── AcquireResult ──

/// Result of a leader lock acquisition attempt.
#[derive(Debug)]
pub enum AcquireResult {
    /// Lock acquired — caller holds a Lease that must be kept alive.
    Acquired(Lease),
    /// Lock held by another tab — caller should subscribe to events() for promotion.
    Waiting,
    /// Lock mechanism unavailable (browser doesn't support Web Locks, etc.).
    Unsupported,
}

/// Release a Web Lock by its ID. Used from pagehide handlers to release the lock
/// when the page goes into bfcache. On non-WASM targets this is a no-op.
#[cfg(target_arch = "wasm32")]
pub fn release_lock_by_id(lock_id: &str) {
    release_web_lock(lock_id);
}

// ── Lease ──

/// Represents exclusive ownership of the leader lock.
///
/// Drops the lock (releases leadership) when dropped.
/// Callers must hold the Lease for the entire leadership duration.
#[derive(Debug)]
pub struct Lease {
    handle: Option<LockHandle>,
    acquired_at: f64,
}

impl Lease {
    fn new(handle: LockHandle) -> Self {
        let acquired_at = {
            #[cfg(target_arch = "wasm32")]
            {
                js_sys::Date::now()
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64() * 1000.0
            }
        };
        Self {
            handle: Some(handle),
            acquired_at,
        }
    }

    /// Timestamp (ms since epoch) when the lease was acquired.
    pub fn acquired_at(&self) -> f64 {
        self.acquired_at
    }

    /// The Web Lock ID (wasm32 only). Used by pagehide handlers to release the lock
    /// when the page goes into bfcache (beforeunload does NOT fire for bfcache).
    #[cfg(target_arch = "wasm32")]
    pub fn lock_id(&self) -> Option<&str> {
        self.handle.as_ref().map(|h| h.id.as_str())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(_handle) = self.handle.take() {
            #[cfg(target_arch = "wasm32")]
            {
                release_web_lock(&_handle.id);
            }

            #[cfg(windows)]
            {
                use windows_sys::Win32::Foundation::CloseHandle;
                use windows_sys::Win32::System::Threading::ReleaseMutex;
                unsafe {
                    ReleaseMutex(_handle);
                    CloseHandle(_handle);
                }
            }

            #[cfg(unix)]
            {
                // File closed on drop → flock released automatically
            }
        }
    }
}

// ── Web Locks JS FFI ──

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(inline_js = r#"
export function acquire_web_lock(lock_name, lock_id, on_acquired, on_released) {
    let release_promise = new Promise((resolve) => {
        window._vaultsync_releases = window._vaultsync_releases || {};
        window._vaultsync_releases[lock_id] = resolve;
    });
    navigator.locks.request(lock_name, async (lock) => {
        on_acquired();
        await release_promise;
        on_released();
    }).catch(err => {
        console.error("Web Lock acquisition failed", err);
    });
}
export function acquire_lock_immediate(lock_name, lock_id, on_acquired, on_released) {
    let outer_resolve;
    let outer_promise = new Promise((resolve) => { outer_resolve = resolve; });
    navigator.locks.request(lock_name, { ifAvailable: true }, async (lock) => {
        if (lock === null) { outer_resolve(false); return; }
        on_acquired();
        outer_resolve(true);
        await new Promise((resolve) => {
            window._vaultsync_releases = window._vaultsync_releases || {};
            window._vaultsync_releases[lock_id] = resolve;
        });
        on_released();
    }).catch(err => {
        console.error("Web Lock acquisition failed", err);
        if (outer_resolve) outer_resolve(false);
    });
    return outer_promise;
}
export function release_web_lock(lock_id) {
    if (window._vaultsync_releases && window._vaultsync_releases[lock_id]) {
        window._vaultsync_releases[lock_id]();
        delete window._vaultsync_releases[lock_id];
    }
}
"#)]
extern "C" {
    fn acquire_web_lock(
        lock_name: &str,
        lock_id: &str,
        on_acquired: &js_sys::Function,
        on_released: &js_sys::Function,
    );
    fn acquire_lock_immediate(
        lock_name: &str,
        lock_id: &str,
        on_acquired: &js_sys::Function,
        on_released: &js_sys::Function,
    ) -> js_sys::Promise;
    fn release_web_lock(lock_id: &str);
}

// ── LeaderElection ──

pub struct LeaderElection {
    namespace: String,
    unique_key: String,
    is_leader: Arc<AtomicBool>,

    /// Lock handle storage (pending or held state).
    lock_handle: std::sync::Mutex<Option<LockHandle>>,

    /// Event bus for leadership events.
    event_bus: Arc<RuntimeBus<LeaderEvent>>,
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
            event_bus: Arc::new(RuntimeBus::new()),
        }
    }

    /// Subscribe to leadership events.
    pub fn events(&self) -> Arc<RuntimeBus<LeaderEvent>> {
        self.event_bus.clone()
    }

    /// Attempt to acquire the leader lock.
    ///
    /// AcquireMode::Immediate — uses `{ifAvailable: true}`. Returns immediately.
    ///   Acquired(Lease): lock acquired and held. Caller is leader.
    ///   Waiting: another tab holds the lock. Caller is follower.
    ///
    /// AcquireMode::Wait — fires a blocking Web Lock request. Returns Waiting
    ///   immediately. When the lock is granted, LeaderEvent::Acquired is emitted
    ///   on the event bus. Caller should subscribe to events() and call
    ///   acquire(Immediate) on receiving Acquired to obtain the Lease.
    pub async fn acquire(&self, mode: AcquireMode) -> Result<AcquireResult, VaultSyncError> {
        match mode {
            AcquireMode::Immediate => self.acquire_immediate().await,
            AcquireMode::Wait => self.acquire_wait(),
        }
    }

    /// Immediate acquisition: `{ifAvailable: true}`, holds lock if free.
    async fn acquire_immediate(&self) -> Result<AcquireResult, VaultSyncError> {
        // Already holding? Return existing lease.
        if self.is_leader() {
            let mut guard = self.lock_handle.lock().unwrap();
            if let Some(handle) = guard.take() {
                return Ok(AcquireResult::Acquired(Lease::new(handle)));
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::JsCast;
            use wasm_bindgen_futures::JsFuture;

            let name = format!("vaultsync-{}-{}", self.namespace, self.unique_key);
            let id = uuid::Uuid::new_v4().to_string();

            let on_acquired = {
                let is_leader_clone = self.is_leader.clone();
                let event_bus = self.event_bus.clone();
                wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                    is_leader_clone.store(true, Ordering::SeqCst);
                    event_bus.publish(&LeaderEvent::Acquired {
                        timestamp_ms: js_sys::Date::now(),
                    });
                }) as Box<dyn Fn()>)
            };

            let on_released = {
                let is_leader_clone = self.is_leader.clone();
                let event_bus = self.event_bus.clone();
                wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                    is_leader_clone.store(false, Ordering::SeqCst);
                    event_bus.publish(&LeaderEvent::Released);
                }) as Box<dyn Fn()>)
            };

            let promise = acquire_lock_immediate(
                &name,
                &id,
                on_acquired.as_ref().unchecked_ref(),
                on_released.as_ref().unchecked_ref(),
            );
            on_acquired.forget();
            on_released.forget();

            match JsFuture::from(promise).await {
                Ok(val) => {
                    let acquired = val.as_bool().unwrap_or(false);
                    if acquired {
                        let state = WasmLockState { name, id };
                        let lease = Lease::new(state);
                        self.is_leader.store(true, Ordering::SeqCst);
                        self.event_bus.publish(&LeaderEvent::Acquired {
                            timestamp_ms: js_sys::Date::now(),
                        });
                        Ok(AcquireResult::Acquired(lease))
                    } else {
                        self.is_leader.store(false, Ordering::SeqCst);
                        self.event_bus.publish(&LeaderEvent::Waiting);
                        Ok(AcquireResult::Waiting)
                    }
                }
                Err(e) => {
                    let msg = format!("acquire_lock_immediate failed: {:?}", e);
                    self.event_bus.publish(&LeaderEvent::Failed(msg.clone()));
                    Err(VaultSyncError::Storage(msg))
                }
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            self.acquire_immediate_sync()
        }
    }

    /// Non-WASM: synchronous immediate acquisition.
    #[cfg(not(target_arch = "wasm32"))]
    fn acquire_immediate_sync(&self) -> Result<AcquireResult, VaultSyncError> {
        let mut handle_guard = self.lock_handle.lock().unwrap();
        if let Some(handle) = handle_guard.take() {
            self.is_leader.store(true, Ordering::SeqCst);
            return Ok(AcquireResult::Acquired(Lease::new(handle)));
        }

        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, ERROR_ALREADY_EXISTS};
            use windows_sys::Win32::System::Threading::CreateMutexW;

            let name = format!("Global\\vaultsync-{}-{}", self.namespace, self.unique_key);
            let mut name_u16: Vec<u16> = name.encode_utf16().collect();
            name_u16.push(0);

            let handle = unsafe { CreateMutexW(std::ptr::null(), 1, name_u16.as_ptr()) };
            if handle == 0 {
                return Ok(AcquireResult::Unsupported);
            }
            let last_err = unsafe { GetLastError() };
            if last_err == ERROR_ALREADY_EXISTS {
                unsafe { CloseHandle(handle); }
                self.event_bus.publish(&LeaderEvent::Waiting);
                return Ok(AcquireResult::Waiting);
            }
            *handle_guard = Some(handle);
            self.is_leader.store(true, Ordering::SeqCst);
            self.event_bus.publish(&LeaderEvent::Acquired {
                timestamp_ms: Self::now_ms(),
            });
            Ok(AcquireResult::Acquired(Lease::new(handle)))
        }

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let lock_path = std::env::temp_dir().join(format!(
                "vaultsync-{}-{}.lock",
                self.namespace, self.unique_key
            ));
            let file = std::fs::OpenOptions::new()
                .read(true).write(true).create(true)
                .open(&lock_path)
                .map_err(|e| VaultSyncError::Storage(format!("failed to open lock file: {e}")))?;
            let fd = file.as_raw_fd();
            let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if ret == 0 {
                self.is_leader.store(true, Ordering::SeqCst);
                self.event_bus.publish(&LeaderEvent::Acquired {
                    timestamp_ms: Self::now_ms(),
                });
                return Ok(AcquireResult::Acquired(Lease::new(file)));
            }
            let last_error = std::io::Error::last_os_error();
            if last_error.kind() == std::io::ErrorKind::WouldBlock
                || last_error.raw_os_error() == Some(libc::EWOULDBLOCK)
                || last_error.raw_os_error() == Some(libc::EAGAIN)
            {
                self.event_bus.publish(&LeaderEvent::Waiting);
                return Ok(AcquireResult::Waiting);
            }
            Err(VaultSyncError::Storage(format!("flock failed: {}", last_error)))
        }

        #[cfg(all(not(any(windows, unix)), not(target_arch = "wasm32")))]
        {
            self.is_leader.store(true, Ordering::SeqCst);
            *handle_guard = Some(());
            Ok(AcquireResult::Acquired(Lease::new(())))
        }
    }

    /// Wait acquisition: fire-and-forget blocking request.
    /// Returns Waiting immediately. LeaderEvent::Acquired fires when lock granted.
    fn acquire_wait(&self) -> Result<AcquireResult, VaultSyncError> {
        // Already holding?
        if self.is_leader() {
            let mut guard = self.lock_handle.lock().unwrap();
            if let Some(handle) = guard.take() {
                return Ok(AcquireResult::Acquired(Lease::new(handle)));
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::JsCast;

            let mut handle_guard = self.lock_handle.lock().unwrap();
            let name = format!("vaultsync-{}-{}", self.namespace, self.unique_key);

            let is_leader_clone = self.is_leader.clone();
            let event_bus = self.event_bus.clone();
            let on_acquired = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                is_leader_clone.store(true, Ordering::SeqCst);
                event_bus.publish(&LeaderEvent::Acquired {
                    timestamp_ms: js_sys::Date::now(),
                });
            }) as Box<dyn Fn()>);

            let is_leader_clone2 = self.is_leader.clone();
            let event_bus2 = self.event_bus.clone();
            let on_released = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                is_leader_clone2.store(false, Ordering::SeqCst);
                event_bus2.publish(&LeaderEvent::Released);
            }) as Box<dyn Fn()>);

            let id = uuid::Uuid::new_v4().to_string();
            let id_clone = id.clone();

            acquire_web_lock(
                &name,
                &id,
                on_acquired.as_ref().unchecked_ref(),
                on_released.as_ref().unchecked_ref(),
            );
            on_acquired.forget();
            on_released.forget();

            *handle_guard = Some(WasmLockState { name, id: id_clone });

            Ok(AcquireResult::Waiting)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            self.acquire_immediate_sync()
        }
    }

    /// Check if this tab currently holds the leader lock.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Set the leader flag externally. Used when a Lease is obtained from one
    /// LeaderElection instance and needs to be reflected in another (e.g., from
    /// the VaultSyncRuntime's immediate-acquire into VaultSyncClient's instance).
    pub fn set_is_leader(&self, val: bool) {
        self.is_leader.store(val, Ordering::SeqCst);
        if val {
            self.event_bus.publish(&LeaderEvent::Acquired {
                timestamp_ms: Self::now_ms(),
            });
        } else {
            self.event_bus.publish(&LeaderEvent::Released);
        }
    }

    /// Synchronous immediate acquisition (non-WASM only).
    /// Returns `true` if lock acquired and we're now the leader.
    /// Used by debug endpoints and CLI tools.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn try_acquire_immediate_sync(&self) -> Result<bool, VaultSyncError> {
        // Drop the lease immediately — caller just wants to check.
        // For real leadership, use acquire(Immediate).await and hold the Lease.
        match self.acquire_immediate_sync()? {
            AcquireResult::Acquired(lease) => {
                drop(lease);
                Ok(true)
            }
            AcquireResult::Waiting => Ok(false),
            AcquireResult::Unsupported => Ok(true),
        }
    }

    // ── Backward compat: try_acquire / release ──
    //
    // These exist to keep existing callers compiling during Sprint 0.
    // They will be removed once all callers use acquire(AcquireMode).

    /// Legacy fire-and-forget lock acquisition.
    /// On WASM: fires blocking request, stores handle internally, returns immediately.
    /// On native: sync attempt, returns true if acquired.
    /// Prefer `acquire(AcquireMode::Immediate).await` in new code.
    pub fn try_acquire(&self) -> Result<bool, VaultSyncError> {
        // Already holding? Return current status
        {
            let guard = self.lock_handle.lock().unwrap();
            if guard.is_some() {
                return Ok(self.is_leader());
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::JsCast;

            let mut handle_guard = self.lock_handle.lock().unwrap();
            let name = format!("vaultsync-{}-{}", self.namespace, self.unique_key);

            let is_leader_clone = self.is_leader.clone();
            let event_bus = self.event_bus.clone();
            let on_acquired = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                is_leader_clone.store(true, Ordering::SeqCst);
                event_bus.publish(&LeaderEvent::Acquired {
                    timestamp_ms: js_sys::Date::now(),
                });
            }) as Box<dyn Fn()>);

            let is_leader_clone2 = self.is_leader.clone();
            let event_bus2 = self.event_bus.clone();
            let on_released = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
                is_leader_clone2.store(false, Ordering::SeqCst);
                event_bus2.publish(&LeaderEvent::Released);
            }) as Box<dyn Fn()>);

            let id = uuid::Uuid::new_v4().to_string();
            let id_clone = id.clone();

            acquire_web_lock(
                &name,
                &id,
                on_acquired.as_ref().unchecked_ref(),
                on_released.as_ref().unchecked_ref(),
            );
            on_acquired.forget();
            on_released.forget();

            *handle_guard = Some(WasmLockState { name, id: id_clone });
            Ok(false)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            self.try_acquire_immediate_sync()
        }
    }

    /// Legacy lock release. Drops the internally stored handle.
    /// Prefer dropping the `Lease` returned by `acquire()` in new code.
    pub fn release(&self) {
        let mut guard = self.lock_handle.lock().unwrap();
        if let Some(_handle) = guard.take() {
            #[cfg(target_arch = "wasm32")]
            {
                release_web_lock(&_handle.id);
            }

            #[cfg(windows)]
            {
                use windows_sys::Win32::Foundation::CloseHandle;
                use windows_sys::Win32::System::Threading::ReleaseMutex;
                unsafe {
                    ReleaseMutex(_handle);
                    CloseHandle(_handle);
                }
            }

            #[cfg(unix)]
            {
                // File closed on drop → flock released automatically
            }
        }
        self.is_leader.store(false, Ordering::SeqCst);
        self.event_bus.publish(&LeaderEvent::Released);
    }

    fn now_ms() -> f64 {
        #[cfg(target_arch = "wasm32")]
        { js_sys::Date::now() }
        #[cfg(not(target_arch = "wasm32"))]
        {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64() * 1000.0
        }
    }
}

impl Drop for LeaderElection {
    fn drop(&mut self) {
        // Any remaining handle is dropped by the struct.
    }
}
