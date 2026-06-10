pub mod push_pull;
pub mod register;
pub mod subscribe;
pub mod snapshots;
pub mod key_management;
pub mod multi_namespace;
pub mod concurrency;
pub mod compaction;

use std::sync::Arc;
use vaultsync_core::coordinator::traits::Coordinator;

pub async fn run_full_conformance_suite(coord: Arc<dyn Coordinator>, prefix: &str) {
    register::run_register_tests(coord.clone(), &format!("{}-reg", prefix)).await;
    push_pull::run_push_pull_tests(coord.clone(), &format!("{}-pp", prefix)).await;
    subscribe::run_subscribe_tests(coord.clone(), &format!("{}-sub", prefix), &format!("{}-sub-other", prefix)).await;
    snapshots::run_snapshots_tests(coord.clone(), &format!("{}-snap", prefix)).await;
    key_management::run_key_management_tests(coord.clone(), &format!("{}-key", prefix)).await;
    multi_namespace::run_multi_namespace_tests(coord.clone(), &format!("{}-mn-a", prefix), &format!("{}-mn-b", prefix)).await;
    concurrency::run_concurrency_tests(coord.clone(), &format!("{}-conc", prefix)).await;
    compaction::run_compaction_tests(coord.clone(), &format!("{}-compact", prefix)).await;
}
