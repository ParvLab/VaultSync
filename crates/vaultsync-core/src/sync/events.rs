use futures::channel::mpsc;

/// Synchronization event channels.
///
/// Both upload and download workers are driven by event notifications
/// rather than polling timers. These channels provide cross-platform
/// wakeup (WASM + native) without tokio dependency leakage.
#[derive(Clone)]
pub struct SyncEvents {
    pub upload_notify: mpsc::UnboundedSender<()>,
    pub download_notify: mpsc::UnboundedSender<()>,
}

impl SyncEvents {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<()>, mpsc::UnboundedReceiver<()>) {
        let (upload_tx, upload_rx) = mpsc::unbounded();
        let (download_tx, download_rx) = mpsc::unbounded();
        (
            Self {
                upload_notify: upload_tx,
                download_notify: download_tx,
            },
            upload_rx,
            download_rx,
        )
    }

    pub fn notify_upload(&self) {
        let _ = self.upload_notify.unbounded_send(());
    }

    pub fn notify_download(&self) {
        let _ = self.download_notify.unbounded_send(());
    }
}
