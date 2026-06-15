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
pub struct ForceSendSync<T>(pub T);

#[cfg(target_arch = "wasm32")]
unsafe impl<T> Send for ForceSendSync<T> {}
#[cfg(target_arch = "wasm32")]
unsafe impl<T> Sync for ForceSendSync<T> {}

#[cfg(target_arch = "wasm32")]
static PENDING_DROPS: std::sync::OnceLock<std::sync::Mutex<Vec<Box<dyn std::any::Any + Send + Sync>>>> = std::sync::OnceLock::new();

#[cfg(target_arch = "wasm32")]
pub fn defer_drop(item: Box<dyn std::any::Any + Send + Sync>) {
    use wasm_bindgen::JsCast;

    let pending = PENDING_DROPS.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    let mut guard = pending.lock().unwrap();
    let is_empty = guard.is_empty();
    guard.push(item);

    if is_empty {
        let window = web_sys::window().expect("no window");
        let closure = wasm_bindgen::closure::Closure::once(move || {
            if let Some(pending) = PENDING_DROPS.get() {
                if let Ok(mut guard) = pending.lock() {
                    guard.clear();
                }
            }
        });
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            0,
        );
        closure.forget();
    }
}

#[cfg(target_arch = "wasm32")]
pub struct SleepFuture {
    rx: futures::channel::oneshot::Receiver<()>,
    timeout_id: i32,
    _closure: Option<wasm_bindgen::closure::Closure<dyn FnMut()>>,
}

#[cfg(target_arch = "wasm32")]
unsafe impl Send for SleepFuture {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for SleepFuture {}

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
        if let Some(closure) = self._closure.take() {
            defer_drop(Box::new(ForceSendSync(closure)));
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
            wasm_bindgen_futures::spawn_local(async move {
                let _ = t.send(());
            });
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
        _closure: Some(closure),
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

#[cfg(target_arch = "wasm32")]
pub struct SendJsFuture<T = wasm_bindgen::JsValue> {
    rx: futures::channel::oneshot::Receiver<Result<wasm_bindgen::JsValue, wasm_bindgen::JsValue>>,
    _closures: Option<(wasm_bindgen::closure::Closure<dyn FnMut(wasm_bindgen::JsValue)>, wasm_bindgen::closure::Closure<dyn FnMut(wasm_bindgen::JsValue)>)>,
    _phantom: std::marker::PhantomData<T>,
}

#[cfg(target_arch = "wasm32")]
unsafe impl<T> Send for SendJsFuture<T> {}
#[cfg(target_arch = "wasm32")]
unsafe impl<T> Sync for SendJsFuture<T> {}

#[cfg(target_arch = "wasm32")]
impl<T: From<wasm_bindgen::JsValue>> std::future::Future for SendJsFuture<T> {
    type Output = Result<T, wasm_bindgen::JsValue>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut rx = unsafe { self.map_unchecked_mut(|s| &mut s.rx) };
        match std::pin::Pin::new(&mut *rx).poll(cx) {
            std::task::Poll::Ready(Ok(Ok(val))) => std::task::Poll::Ready(Ok(val.into())),
            std::task::Poll::Ready(Ok(Err(err))) => std::task::Poll::Ready(Err(err)),
            std::task::Poll::Ready(Err(_)) => std::task::Poll::Ready(Err(wasm_bindgen::JsValue::from_str("SendJsFuture cancelled"))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T> Drop for SendJsFuture<T> {
    fn drop(&mut self) {
        if let Some((onsuccess, onerror)) = self._closures.take() {
            defer_drop(Box::new(ForceSendSync(onsuccess)));
            defer_drop(Box::new(ForceSendSync(onerror)));
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T: wasm_bindgen::convert::FromWasmAbi + 'static> From<js_sys::Promise<T>> for SendJsFuture<T> {
    fn from(p: js_sys::Promise<T>) -> Self {
        let js_val: wasm_bindgen::JsValue = p.into();
        let promise: js_sys::Promise = js_val.into();
        let (tx, rx) = futures::channel::oneshot::channel::<Result<wasm_bindgen::JsValue, wasm_bindgen::JsValue>>();
        let shared_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));

        let tx_success = shared_tx.clone();
        let onsuccess = wasm_bindgen::closure::Closure::wrap(Box::new(move |val: wasm_bindgen::JsValue| {
            let tx_opt = {
                let mut guard = tx_success.lock().unwrap();
                guard.take()
            };
            if let Some(tx) = tx_opt {
                let _ = tx.send(Ok(val));
            }
        }) as Box<dyn FnMut(wasm_bindgen::JsValue)>);

        let tx_error = shared_tx.clone();
        let onerror = wasm_bindgen::closure::Closure::wrap(Box::new(move |val: wasm_bindgen::JsValue| {
            let tx_opt = {
                let mut guard = tx_error.lock().unwrap();
                guard.take()
            };
            if let Some(tx) = tx_opt {
                let _ = tx.send(Err(val));
            }
        }) as Box<dyn FnMut(wasm_bindgen::JsValue)>);

        let _ = promise.then2(&onsuccess, &onerror);

        Self {
            rx,
            _closures: Some((onsuccess, onerror)),
            _phantom: std::marker::PhantomData,
        }
    }
}
