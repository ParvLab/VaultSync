#[cfg(target_arch = "wasm32")]
pub fn system_time_now_ms() -> u64 {
    js_sys::Date::now() as u64
}

#[cfg(not(target_arch = "wasm32"))]
pub fn system_time_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn system_time_now_secs() -> u64 {
    system_time_now_ms() / 1000
}

#[cfg(target_arch = "wasm32")]
pub fn spawn<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: std::time::Duration) {
    let mut cb = |resolve: js_sys::Function, _reject: js_sys::Function| {
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                &resolve,
                duration.as_millis() as i32,
            );
        }
    };
    let promise = js_sys::Promise::new(&mut cb);
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: std::time::Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlatformInstant(u64); // Milliseconds since epoch

#[cfg(target_arch = "wasm32")]
impl PlatformInstant {
    pub fn now() -> Self {
        Self(js_sys::Date::now() as u64)
    }

    pub fn elapsed(&self) -> std::time::Duration {
        let diff = (js_sys::Date::now() as u64).saturating_sub(self.0);
        std::time::Duration::from_millis(diff)
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::Add<std::time::Duration> for PlatformInstant {
    type Output = Self;
    fn add(self, rhs: std::time::Duration) -> Self {
        Self(self.0 + rhs.as_millis() as u64)
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::Sub<std::time::Duration> for PlatformInstant {
    type Output = Self;
    fn sub(self, rhs: std::time::Duration) -> Self {
        Self(self.0.saturating_sub(rhs.as_millis() as u64))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub type PlatformInstant = std::time::Instant;
