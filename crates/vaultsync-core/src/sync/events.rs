use crate::coordinator::traits::PendingMutation;
use futures::channel::mpsc;

/// Synchronization event channels.
///
/// Both upload and download workers are driven by event notifications
/// rather than polling timers. These channels provide cross-platform
/// wakeup (WASM + native) without tokio dependency leakage.
#[derive(Clone)]
pub struct SyncEvents {
    pub upload_notify: mpsc::UnboundedSender<()>,
    /// Push mutations arrive here from the coordinator subscription
    /// or WS transport. The download worker processes them immediately
    /// and advances the cursor. `None` is a wakeup-only signal (bootstrap / timer).
    pub download_notify: mpsc::UnboundedSender<Option<PendingMutation>>,
}

impl SyncEvents {
    pub fn new(
    ) -> (
        Self,
        mpsc::UnboundedReceiver<()>,
        mpsc::UnboundedReceiver<Option<PendingMutation>>,
    ) {
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

    /// Wake the download worker. If a pending mutation is provided,
    /// it will be processed immediately; otherwise the worker just
    /// runs the next pull batch cycle.
    pub fn notify_download(&self, push: Option<PendingMutation>) {
        let _ = self.download_notify.unbounded_send(push);
    }
}
