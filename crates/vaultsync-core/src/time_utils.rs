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
pub struct SleepFuture {
    rx: futures::channel::oneshot::Receiver<()>,
    timeout_id: i32,
    _closure: wasm_bindgen::closure::Closure<dyn FnMut()>,
}

#[cfg(target_arch = "wasm32")]
impl std::future::Future for SleepFuture {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.rx).poll(cx) {
            std::task::Poll::Ready(_) => std::task::Poll::Ready(()),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for SleepFuture {
    fn drop(&mut self) {
        if let Some(window) = web_sys::window() {
            window.clear_timeout_with_handle(self.timeout_id);
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub fn sleep(duration: std::time::Duration) -> SleepFuture {
    use wasm_bindgen::JsCast;
    let (tx, rx) = futures::channel::oneshot::channel();
    let mut tx_opt = Some(tx);
    let closure = wasm_bindgen::closure::Closure::wrap(Box::new(move || {
        if let Some(t) = tx_opt.take() {
            let _ = t.send(());
        }
    }) as Box<dyn FnMut()>);

    let window = web_sys::window().expect("no window");
    let timeout_id = window
        .set_timeout_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            duration.as_millis() as i32,
        )
        .expect("set_timeout failed");

    SleepFuture {
        rx,
        timeout_id,
        _closure: closure,
    }
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
