use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use vaultsync_core::crdt::document::CRDTDocument;
use vaultsync_core::crdt::types::CrdtValue;
use vaultsync_core::storage::traits::{
    DocumentReader, MutationSink, ReplayContext, ReplayReport, ReplaySource,
};
use vaultsync_core::VaultSyncError;

use crate::migration::DocEntry;
use crate::page_store::PageId;
use crate::storage_runtime::StorageRuntime;

/// Reads documents from StorageRuntime (sole OPFS gatekeeper) and returns decoded CRDT records.
/// This is the concrete DocumentReader that routes through StorageRuntime.
pub struct StorageDocumentReader {
    storage: Arc<StorageRuntime>,
    store_name: &'static str,
}

impl StorageDocumentReader {
    pub fn new(storage: Arc<StorageRuntime>, store_name: &'static str) -> Self {
        Self { storage, store_name }
    }
}

#[async_trait]
impl DocumentReader for StorageDocumentReader {
    async fn read_all_records(
        &self,
        _namespace: &str,
    ) -> Result<Vec<(String, String, HashMap<String, CrdtValue>)>, VaultSyncError> {
        let mut results = Vec::new();
        let page_ids = self.storage.list_page_ids(self.store_name, "read_all_records").await?;
        if page_ids.is_empty() {
            return Ok(results);
        }

        // Read all pages sequentially and extract DocEntry records
        for page_id in &page_ids {
            let page_data = match self.storage.read_page(self.store_name, *page_id).await {
                Ok(Some(data)) => data,
                _ => continue,
            };
            let doc_entry: DocEntry = match postcard::from_bytes(&page_data) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let doc = match CRDTDocument::from_snapshot(&doc_entry.bytes) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let map = doc.to_map();
            results.push((doc_entry.doc_id, doc_entry.record_id, map));
        }
        Ok(results)
    }
}

/// Orchestrates document replay from storage to a MutationSink.
/// Owns a DocumentReader and knows nothing about PageStore or OPFS.
pub struct ReplayEngine {
    reader: Arc<dyn DocumentReader>,
}

impl ReplayEngine {
    pub fn new(reader: Arc<dyn DocumentReader>) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl ReplaySource for ReplayEngine {
    async fn replay_since(
        &self,
        ctx: &ReplayContext,
        sink: &dyn MutationSink,
    ) -> ReplayReport {
        let t0 = js_sys::Date::now();

        let mut report = ReplayReport {
            namespace: ctx.namespace.clone(),
            requested_cursor: ctx.cursor,
            storage_cursor: 0,
            documents_scanned: 0,
            records_scanned: 0,
            pages_scanned: 0,
            decoded: 0,
            emitted: 0,
            skipped: HashMap::new(),
            elapsed_ms: 0,
        };

        let records = match self.reader.read_all_records(&ctx.namespace).await {
            Ok(records) => {
                report.documents_scanned = records.len() as u64;
                records
            }
            Err(e) => {
                engine_warn!("[ReplayEngine] read_all_records failed: {:?}", e);
                report.elapsed_ms = (js_sys::Date::now() - t0) as u64;
                return report;
            }
        };

        for (doc_id, record_id, fields) in &records {
            report.records_scanned += 1;
            sink.send_mutation(doc_id, record_id, fields);
            report.emitted += 1;
        }
        report.decoded = report.records_scanned;
        // Note: caller owns SYNC_DONE and cursor — ReplayEngine only emits MUTATION frames

        report.elapsed_ms = (js_sys::Date::now() - t0) as u64;
        report
    }
}
