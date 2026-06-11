use super::traits::*;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct MockCoordinator {
    push_results: std::sync::Mutex<Vec<Result<Vec<SequenceId>, CoordinatorError>>>,
    pull_results: std::sync::Mutex<Vec<Result<Vec<PendingMutation>, CoordinatorError>>>,
    push_index: AtomicUsize,
    pull_index: AtomicUsize,
}

impl MockCoordinator {
    pub fn new() -> Self {
        Self {
            push_results: std::sync::Mutex::new(Vec::new()),
            pull_results: std::sync::Mutex::new(Vec::new()),
            push_index: AtomicUsize::new(0),
            pull_index: AtomicUsize::new(0),
        }
    }

    pub fn expect_push(&self, result: Result<Vec<SequenceId>, CoordinatorError>) {
        self.push_results.lock().unwrap().push(result);
    }

    pub fn expect_pull(&self, result: Result<Vec<PendingMutation>, CoordinatorError>) {
        self.pull_results.lock().unwrap().push(result);
    }
}

#[async_trait]
impl Coordinator for MockCoordinator {
    async fn push(
        &self,
        _namespace: &str,
        _mutations: Vec<EncryptedMutation>,
    ) -> Result<Vec<SequenceId>, CoordinatorError> {
        let i = self.push_index.fetch_add(1, Ordering::SeqCst);
        let results = self.push_results.lock().unwrap();
        results.get(i).cloned().unwrap_or(Ok(vec![]))
    }

    async fn pull(
        &self,
        _namespace: &str,
        _after: SequenceId,
        _limit: usize,
    ) -> Result<Vec<PendingMutation>, CoordinatorError> {
        let i = self.pull_index.fetch_add(1, Ordering::SeqCst);
        let results = self.pull_results.lock().unwrap();
        results.get(i).cloned().unwrap_or(Ok(vec![]))
    }

    async fn subscribe(
        &self,
        _namespace: &str,
        _from_sequence: SequenceId,
    ) -> Result<Box<dyn Stream<Item = PendingMutation> + Send>, CoordinatorError> {
        let rx = tokio::sync::mpsc::unbounded_channel().1;
        Ok(Box::new(MockSubscription { rx: Some(rx) }))
    }

    async fn register(&self, _namespace: &str, _info: ReplicaInfo) -> Result<(), CoordinatorError> {
        Ok(())
    }

    async fn heartbeat(&self, _namespace: &str, _replica_id: &str) -> Result<(), CoordinatorError> {
        Ok(())
    }

    async fn schema_version(&self, _namespace: &str) -> Result<u64, CoordinatorError> {
        Ok(0)
    }
}

struct MockSubscription {
    rx: Option<tokio::sync::mpsc::UnboundedReceiver<PendingMutation>>,
}

impl Stream for MockSubscription {
    type Item = PendingMutation;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.rx {
            Some(rx) => rx.poll_recv(cx),
            None => Poll::Ready(None),
        }
    }
}
