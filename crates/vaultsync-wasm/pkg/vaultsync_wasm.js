/* @ts-self-types="./vaultsync_wasm.d.ts" */
import { acquire_lock_immediate, acquire_web_lock, release_web_lock } from './snippets/vaultsync-core-569f5a5e15e207ef/inline0.js';


/**
 * @enum {0}
 */
export const ClientEvent = Object.freeze({
    StatusDirty: 0, "0": "StatusDirty",
});

export class EventBusProxy {
    static __wrap(ptr) {
        const obj = Object.create(EventBusProxy.prototype);
        obj.__wbg_ptr = ptr;
        EventBusProxyFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        EventBusProxyFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_eventbusproxy_free(ptr, 0);
    }
    /**
     * @returns {number}
     */
    subscriberCount() {
        const ret = wasm.eventbusproxy_subscriberCount(this.__wbg_ptr);
        return ret >>> 0;
    }
}
if (Symbol.dispose) EventBusProxy.prototype[Symbol.dispose] = EventBusProxy.prototype.free;

/**
 * Manages multi-tab presence awareness via BroadcastChannel.
 *
 * Each tab announces join/leave/heartbeat so peers know who is active.
 * Exported to JavaScript via `#[wasm_bindgen]`.
 */
export class PresenceManager {
    static __wrap(ptr) {
        const obj = Object.create(PresenceManager.prototype);
        obj.__wbg_ptr = ptr;
        PresenceManagerFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        PresenceManagerFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_presencemanager_free(ptr, 0);
    }
    /**
     * Returns a JSON string of active peers: `{"replica_id": last_seen_ms, ...}`.
     * @returns {string}
     */
    activePeers() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.presencemanager_activePeers(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Create and start announcing presence on a dedicated BroadcastChannel.
     * The channel name is `"vaultsync-presence-{namespace}"`.
     * @param {string} namespace
     * @param {string} replica_id
     */
    constructor(namespace, replica_id) {
        const ptr0 = passStringToWasm0(namespace, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(replica_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.presencemanager_new(ptr0, len0, ptr1, len1);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        this.__wbg_ptr = ret[0];
        PresenceManagerFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Number of visible peers (excluding self).
     * @returns {number}
     */
    peer_count() {
        const ret = wasm.presencemanager_peer_count(this.__wbg_ptr);
        return ret >>> 0;
    }
}
if (Symbol.dispose) PresenceManager.prototype[Symbol.dispose] = PresenceManager.prototype.free;

export class ReplicationNamespace {
    static __wrap(ptr) {
        const obj = Object.create(ReplicationNamespace.prototype);
        obj.__wbg_ptr = ptr;
        ReplicationNamespaceFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ReplicationNamespaceFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_replicationnamespace_free(ptr, 0);
    }
    clearPending() {
        wasm.replicationnamespace_clearPending(this.__wbg_ptr);
    }
    /**
     * @returns {number}
     */
    pendingCount() {
        const ret = wasm.replicationnamespace_pendingCount(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * @param {number} limit
     * @returns {string}
     */
    predictNext(limit) {
        let deferred2_0;
        let deferred2_1;
        try {
            const ret = wasm.replicationnamespace_predictNext(this.__wbg_ptr, limit);
            var ptr1 = ret[0];
            var len1 = ret[1];
            if (ret[3]) {
                ptr1 = 0; len1 = 0;
                throw takeFromExternrefTable0(ret[2]);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
}
if (Symbol.dispose) ReplicationNamespace.prototype[Symbol.dispose] = ReplicationNamespace.prototype.free;

/**
 * SyncStateStore that reads/writes cursor+generation in memory AND persists
 * every write through the MetadataStore-backed Storage trait.
 * Legacy PersistentSyncStateStore is replaced by InMemorySyncStateStore.
 * Cursor and generation are persisted via MetadataRuntime on checkpoint,
 * not on every cursor update. This eliminates 2 OPFS writes per mutation.
 */
export class VaultSyncRuntime {
    static __wrap(ptr) {
        const obj = Object.create(VaultSyncRuntime.prototype);
        obj.__wbg_ptr = ptr;
        VaultSyncRuntimeFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        VaultSyncRuntimeFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_vaultsyncruntime_free(ptr, 0);
    }
    /**
     * @returns {bigint}
     */
    active_key_version() {
        const ret = wasm.vaultsyncruntime_active_key_version(this.__wbg_ptr);
        return BigInt.asUintN(64, ret);
    }
    /**
     * Begin hydration phase. Must be paired with markReady().
     */
    beginHydration() {
        wasm.vaultsyncruntime_beginHydration(this.__wbg_ptr);
    }
    /**
     * Check if cache eviction is needed.
     * @returns {boolean}
     */
    cacheNeedsEviction() {
        const ret = wasm.vaultsyncruntime_cacheNeedsEviction(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Get cache stats as JSON.
     * @returns {string}
     */
    cacheStats() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.vaultsyncruntime_cacheStats(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Phase 4f: Check if mirror should promote to leader (leader heartbeat timeout).
     * Returns true if leader is gone and JS should reinitialize with new_with_coordinator().
     * Legacy — prefer waitForPromotion() for event-driven usage.
     * @returns {boolean}
     */
    checkMirrorPromotion() {
        const ret = wasm.vaultsyncruntime_checkMirrorPromotion(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Runs compaction on the given namespace, returns JSON stats.
     * @param {string} namespace
     * @returns {Promise<string>}
     */
    compactNamespace(namespace) {
        const ptr0 = passStringToWasm0(namespace, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_compactNamespace(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * @param {string} doc_id
     * @param {string} schema_json
     * @returns {Promise<void>}
     */
    define_schema(doc_id, schema_json) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(schema_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_define_schema(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * @param {string} doc_id
     * @param {string} record_id
     * @returns {Promise<void>}
     */
    delete(doc_id, record_id) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(record_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_delete(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Force-demote this tab to Follower when LEADER_ELECTED is received from another tab.
     * Creates a new MirrorRuntime + BC handler + LeaderElection so the tab can
     * continue processing mutations and detect when to re-promote.
     * Called from JS BC handler. Safe to call even if not currently leader (no-op in that case).
     * @returns {Promise<void>}
     */
    demote() {
        const ret = wasm.vaultsyncruntime_demote(this.__wbg_ptr);
        return ret;
    }
    /**
     * Returns the event bus for publishing/subscribing to engine events.
     * @returns {EventBusProxy}
     */
    events() {
        const ret = wasm.vaultsyncruntime_events(this.__wbg_ptr);
        return EventBusProxy.__wrap(ret);
    }
    /**
     * @param {string} doc_id
     * @returns {Promise<Array<any>>}
     */
    find(doc_id) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_find(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * @param {string} doc_id
     * @param {string} record_id
     * @returns {Promise<void>}
     */
    fire_subscription(doc_id, record_id) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(record_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_fire_subscription(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Phase 5: Flush pending uploads through the UploadScheduler.
     * Drains pending mutations from the debounced queue and processes them
     * through VaultSyncClient. Returns JSON with processed count and status.
     * Call this periodically (e.g., every 100ms via JS setInterval).
     * @returns {Promise<string>}
     */
    flushPendingUploads() {
        const ret = wasm.vaultsyncruntime_flushPendingUploads(this.__wbg_ptr);
        return ret;
    }
    /**
     * @param {string} doc_id
     * @param {string} record_id
     * @returns {Promise<any>}
     */
    get(doc_id, record_id) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(record_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_get(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Leader processes a command from a follower: executes the mutation through the normal
     * VaultSyncClient pipeline, then broadcasts invalidation to all tabs.
     * @param {string} verb
     * @param {string} doc_id
     * @param {string} record_id
     * @param {string} json
     * @returns {Promise<void>}
     */
    handle_command(verb, doc_id, record_id, json) {
        const ptr0 = passStringToWasm0(verb, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(record_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len3 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_handle_command(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3);
        return ret;
    }
    /**
     * @param {string} doc_id
     * @param {string} record_id
     * @param {string} json
     * @returns {Promise<void>}
     */
    insert(doc_id, record_id, json) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(record_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_insert(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
        return ret;
    }
    /**
     * @returns {boolean}
     */
    is_leader() {
        const ret = wasm.vaultsyncruntime_is_leader(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Phase 6: Returns last compaction duration in ms, or 0 if never run.
     * @returns {bigint}
     */
    lastCompactionMs() {
        const ret = wasm.vaultsyncruntime_lastCompactionMs(this.__wbg_ptr);
        return BigInt.asUintN(64, ret);
    }
    /**
     * List active namespaces as JSON array.
     * @returns {any[]}
     */
    listNamespaces() {
        const ret = wasm.vaultsyncruntime_listNamespaces(this.__wbg_ptr);
        var v1 = getArrayJsValueFromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
        return v1;
    }
    /**
     * @returns {any}
     */
    list_key_versions() {
        const ret = wasm.vaultsyncruntime_list_key_versions(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Run a single maintenance tick. Returns number of phases that ran.
     * @returns {number}
     */
    maintenanceTick() {
        const ret = wasm.vaultsyncruntime_maintenanceTick(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Mark hydration complete. Unblocks any waiting find()/get() calls.
     * Leader broadcasts LEADER_READY when marked ready.
     */
    markReady() {
        wasm.vaultsyncruntime_markReady(this.__wbg_ptr);
    }
    /**
     * Returns a JSON snapshot of all metrics counters.
     * Phase 4: Returns mirror metrics in follower mode.
     * @returns {string}
     */
    metricsSnapshot() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.vaultsyncruntime_metricsSnapshot(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Phase 3: Check mirror paused state and drain_seq for promotion diagnostics.
     * @returns {any}
     */
    mirrorState() {
        const ret = wasm.vaultsyncruntime_mirrorState(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * @param {string} namespace
     * @param {string} replica_id
     * @param {string | null} [db_name]
     * @param {string | null} [storage_backend]
     * @returns {Promise<VaultSyncRuntime>}
     */
    static new(namespace, replica_id, db_name, storage_backend) {
        const ptr0 = passStringToWasm0(namespace, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(replica_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(db_name) ? 0 : passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len2 = WASM_VECTOR_LEN;
        var ptr3 = isLikeNone(storage_backend) ? 0 : passStringToWasm0(storage_backend, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len3 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_new(ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3);
        return ret;
    }
    /**
     * @param {string} namespace
     * @param {string} replica_id
     * @param {string} coordinator_url
     * @param {string | null} [auth_token]
     * @param {string | null} [db_name]
     * @param {string | null} [storage_backend]
     * @returns {Promise<VaultSyncRuntime>}
     */
    static new_with_coordinator(namespace, replica_id, coordinator_url, auth_token, db_name, storage_backend) {
        const ptr0 = passStringToWasm0(namespace, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(replica_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(coordinator_url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        var ptr3 = isLikeNone(auth_token) ? 0 : passStringToWasm0(auth_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len3 = WASM_VECTOR_LEN;
        var ptr4 = isLikeNone(db_name) ? 0 : passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len4 = WASM_VECTOR_LEN;
        var ptr5 = isLikeNone(storage_backend) ? 0 : passStringToWasm0(storage_backend, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len5 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_new_with_coordinator(ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3, ptr4, len4, ptr5, len5);
        return ret;
    }
    /**
     * @param {Function} callback
     */
    on_event(callback) {
        wasm.vaultsyncruntime_on_event(this.__wbg_ptr, callback);
    }
    /**
     * Phase 3: Pause mirror mutation processing for deterministic promotion snapshot.
     * After pause(), no BC mutations will be applied. Returns the current drain_seq
     * value so JS can verify the queue is quiescent.
     * @returns {bigint}
     */
    pauseMirror() {
        const ret = wasm.vaultsyncruntime_pauseMirror(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return BigInt.asUintN(64, ret[0]);
    }
    /**
     * Returns a clone of the PresenceManager if available.
     * @returns {PresenceManager | undefined}
     */
    presence() {
        const ret = wasm.vaultsyncruntime_presence(this.__wbg_ptr);
        return ret === 0 ? undefined : PresenceManager.__wrap(ret);
    }
    /**
     * Phase 4b: Promote this follower tab to leader in-process (6-phase pipeline).
     * Called by JS when waitForPromotion() resolves.
     * Phases: Acquire → Construct → Restore → Writable → Install → Announce.
     * Each phase populates PromoteCtx fields; the pipeline guarantees ordering.
     * @param {string} namespace
     * @param {string} coordinator_url
     * @param {string | null} [auth_token]
     * @param {string | null} [db_name]
     * @param {string | null} [storage_backend]
     * @returns {Promise<void>}
     */
    promote(namespace, coordinator_url, auth_token, db_name, storage_backend) {
        const ptr0 = passStringToWasm0(namespace, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(coordinator_url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(auth_token) ? 0 : passStringToWasm0(auth_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len2 = WASM_VECTOR_LEN;
        var ptr3 = isLikeNone(db_name) ? 0 : passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len3 = WASM_VECTOR_LEN;
        var ptr4 = isLikeNone(storage_backend) ? 0 : passStringToWasm0(storage_backend, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len4 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_promote(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3, ptr4, len4);
        return ret;
    }
    /**
     * @param {number} keep_versions
     */
    prune_key_versions(keep_versions) {
        const ret = wasm.vaultsyncruntime_prune_key_versions(this.__wbg_ptr, keep_versions);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Returns the replication namespace proxy.
     * @returns {ReplicationNamespace}
     */
    replication() {
        const ret = wasm.vaultsyncruntime_replication(this.__wbg_ptr);
        return ReplicationNamespace.__wrap(ret);
    }
    /**
     * Runs the resource manager sweep (recompute access scores, promote/demote tiers).
     * Returns JSON with tier byte counts, promotions, demotions, eviction candidates.
     * @returns {Promise<string>}
     */
    resourceSweep() {
        const ret = wasm.vaultsyncruntime_resourceSweep(this.__wbg_ptr);
        return ret;
    }
    /**
     * Phase 3: Resume mirror mutation processing (abort promotion).
     */
    resumeMirror() {
        const ret = wasm.vaultsyncruntime_resumeMirror(this.__wbg_ptr);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * @returns {Promise<any>}
     */
    rotate_keys() {
        const ret = wasm.vaultsyncruntime_rotate_keys(this.__wbg_ptr);
        return ret;
    }
    /**
     * Runs lifecycle (tombstone cleanup) on the given namespace, returns JSON stats.
     * @param {string} namespace
     * @returns {Promise<string>}
     */
    runLifecycle(namespace) {
        const ptr0 = passStringToWasm0(namespace, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_runLifecycle(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * @returns {string}
     */
    runtimeState() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.vaultsyncruntime_runtimeState(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Returns the current runtime status as a string.
     * Sprint D: Enhanced runtime status with health, namespace, lag, and leader identity.
     * Possible values for "mode": "Leader", "Mirror", "Promoting", "Recovering", "Connecting", "Offline"
     * @returns {string}
     */
    runtimeStatus() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.vaultsyncruntime_runtimeStatus(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Engine V2: expose Runtime + PersistenceEngine health stats to JS
     * @returns {any}
     */
    runtime_storage_stats() {
        const ret = wasm.vaultsyncruntime_runtime_storage_stats(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * @returns {Promise<void>}
     */
    shutdown() {
        const ret = wasm.vaultsyncruntime_shutdown(this.__wbg_ptr);
        return ret;
    }
    /**
     * Returns JSON with storage statistics (page count, segment state, etc.).
     * @returns {Promise<string>}
     */
    storageStats() {
        const ret = wasm.vaultsyncruntime_storageStats(this.__wbg_ptr);
        return ret;
    }
    /**
     * @param {string} doc_id
     * @param {Function} callback
     * @returns {WasmSubscriptionHandle}
     */
    subscribe(doc_id, callback) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_subscribe(this.__wbg_ptr, ptr0, len0, callback);
        return WasmSubscriptionHandle.__wrap(ret);
    }
    /**
     * @returns {Promise<any>}
     */
    sync_status() {
        const ret = wasm.vaultsyncruntime_sync_status(this.__wbg_ptr);
        return ret;
    }
    /**
     * Phase 6: Try auto-compaction via CompactionScheduler.
     * Returns JSON with compaction stats, or null if no compaction was needed.
     * JS should call this periodically (e.g., every 30s via setInterval).
     * @returns {Promise<any>}
     */
    tryCompact() {
        const ret = wasm.vaultsyncruntime_tryCompact(this.__wbg_ptr);
        return ret;
    }
    /**
     * @param {WasmSubscriptionHandle} handle
     */
    unsubscribe(handle) {
        _assertClass(handle, WasmSubscriptionHandle);
        const ret = wasm.vaultsyncruntime_unsubscribe(this.__wbg_ptr, handle.__wbg_ptr);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * @param {string} doc_id
     * @param {string} record_id
     * @param {string} json
     * @returns {Promise<void>}
     */
    update(doc_id, record_id, json) {
        const ptr0 = passStringToWasm0(doc_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(record_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.vaultsyncruntime_update(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
        return ret;
    }
    /**
     * Step 4: Wait for promotion signal via oneshot channel (event-driven, no polling).
     * Resolves when LeaderEvent::Acquired fires (Web Lock granted to this follower).
     * JS await this, then calls promote(). Returns immediately if already signaled.
     * @returns {Promise<void>}
     */
    waitForPromotion() {
        const ret = wasm.vaultsyncruntime_waitForPromotion(this.__wbg_ptr);
        return ret;
    }
    /**
     * Returns the working sets namespace proxy for CRUD operations.
     * @returns {WorkingSetsNamespace}
     */
    workingSets() {
        const ret = wasm.vaultsyncruntime_workingSets(this.__wbg_ptr);
        return WorkingSetsNamespace.__wrap(ret);
    }
    /**
     * Returns the workspace namespace proxy for CRUD operations.
     * @returns {WorkspaceNamespace}
     */
    workspace() {
        const ret = wasm.vaultsyncruntime_workspace(this.__wbg_ptr);
        return WorkspaceNamespace.__wrap(ret);
    }
}
if (Symbol.dispose) VaultSyncRuntime.prototype[Symbol.dispose] = VaultSyncRuntime.prototype.free;

export class WasmIPC {
    static __wrap(ptr) {
        const obj = Object.create(WasmIPC.prototype);
        obj.__wbg_ptr = ptr;
        WasmIPCFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmIPCFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmipc_free(ptr, 0);
    }
    /**
     * @param {string} channel_name
     * @returns {WasmIPC}
     */
    static new(channel_name) {
        const ptr0 = passStringToWasm0(channel_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmipc_new(ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return WasmIPC.__wrap(ret[0]);
    }
    /**
     * Create WasmIPC from an existing BroadcastChannel (used by client integration)
     * @param {BroadcastChannel} channel
     * @returns {WasmIPC}
     */
    static new_with_channel(channel) {
        const ret = wasm.wasmipc_new_with_channel(channel);
        return WasmIPC.__wrap(ret);
    }
    /**
     * @param {Function} callback
     */
    on_message(callback) {
        wasm.wasmipc_on_message(this.__wbg_ptr, callback);
    }
    /**
     * @returns {string}
     */
    receive() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.wasmipc_receive(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @param {string} msg
     */
    send(msg) {
        const ptr0 = passStringToWasm0(msg, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmipc_send(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
}
if (Symbol.dispose) WasmIPC.prototype[Symbol.dispose] = WasmIPC.prototype.free;

export class WasmSubscriptionHandle {
    static __wrap(ptr) {
        const obj = Object.create(WasmSubscriptionHandle.prototype);
        obj.__wbg_ptr = ptr;
        WasmSubscriptionHandleFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmSubscriptionHandleFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmsubscriptionhandle_free(ptr, 0);
    }
    /**
     * @param {VaultSyncRuntime} client
     */
    cancel(client) {
        _assertClass(client, VaultSyncRuntime);
        const ret = wasm.wasmsubscriptionhandle_cancel(this.__wbg_ptr, client.__wbg_ptr);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
}
if (Symbol.dispose) WasmSubscriptionHandle.prototype[Symbol.dispose] = WasmSubscriptionHandle.prototype.free;

export class WorkingSetsNamespace {
    static __wrap(ptr) {
        const obj = Object.create(WorkingSetsNamespace.prototype);
        obj.__wbg_ptr = ptr;
        WorkingSetsNamespaceFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WorkingSetsNamespaceFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_workingsetsnamespace_free(ptr, 0);
    }
    clearActive() {
        wasm.workingsetsnamespace_clearActive(this.__wbg_ptr);
    }
    /**
     * @param {string} name
     * @param {string} filter_json
     * @param {bigint | null} [workspace_id]
     * @returns {bigint}
     */
    create(name, filter_json, workspace_id) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(filter_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.workingsetsnamespace_create(this.__wbg_ptr, ptr0, len0, ptr1, len1, !isLikeNone(workspace_id), isLikeNone(workspace_id) ? BigInt(0) : workspace_id);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return BigInt.asUintN(64, ret[0]);
    }
    /**
     * @param {bigint} id
     * @returns {boolean}
     */
    delete(id) {
        const ret = wasm.workingsetsnamespace_delete(this.__wbg_ptr, id);
        return ret !== 0;
    }
    /**
     * @returns {any}
     */
    getActive() {
        const ret = wasm.workingsetsnamespace_getActive(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    list() {
        let deferred2_0;
        let deferred2_1;
        try {
            const ret = wasm.workingsetsnamespace_list(this.__wbg_ptr);
            var ptr1 = ret[0];
            var len1 = ret[1];
            if (ret[3]) {
                ptr1 = 0; len1 = 0;
                throw takeFromExternrefTable0(ret[2]);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * @param {bigint} id
     * @returns {boolean}
     */
    setActive(id) {
        const ret = wasm.workingsetsnamespace_setActive(this.__wbg_ptr, id);
        return ret !== 0;
    }
}
if (Symbol.dispose) WorkingSetsNamespace.prototype[Symbol.dispose] = WorkingSetsNamespace.prototype.free;

export class WorkspaceNamespace {
    static __wrap(ptr) {
        const obj = Object.create(WorkspaceNamespace.prototype);
        obj.__wbg_ptr = ptr;
        WorkspaceNamespaceFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WorkspaceNamespaceFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_workspacenamespace_free(ptr, 0);
    }
    /**
     * @param {string} name
     * @param {string} namespace
     * @param {string} schema_json
     * @param {string} retention_json
     * @returns {bigint}
     */
    create(name, namespace, schema_json, retention_json) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(namespace, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(schema_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(retention_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len3 = WASM_VECTOR_LEN;
        const ret = wasm.workspacenamespace_create(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return BigInt.asUintN(64, ret[0]);
    }
    /**
     * @param {bigint} id
     * @returns {boolean}
     */
    delete(id) {
        const ret = wasm.workspacenamespace_delete(this.__wbg_ptr, id);
        return ret !== 0;
    }
    /**
     * @param {bigint} id
     * @returns {string}
     */
    get(id) {
        let deferred2_0;
        let deferred2_1;
        try {
            const ret = wasm.workspacenamespace_get(this.__wbg_ptr, id);
            var ptr1 = ret[0];
            var len1 = ret[1];
            if (ret[3]) {
                ptr1 = 0; len1 = 0;
                throw takeFromExternrefTable0(ret[2]);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * @returns {string}
     */
    list() {
        let deferred2_0;
        let deferred2_1;
        try {
            const ret = wasm.workspacenamespace_list(this.__wbg_ptr);
            var ptr1 = ret[0];
            var len1 = ret[1];
            if (ret[3]) {
                ptr1 = 0; len1 = 0;
                throw takeFromExternrefTable0(ret[2]);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * @param {bigint} id
     * @param {string | null} [name]
     * @param {string | null} [retention_json]
     * @returns {boolean}
     */
    update(id, name, retention_json) {
        var ptr0 = isLikeNone(name) ? 0 : passStringToWasm0(name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(retention_json) ? 0 : passStringToWasm0(retention_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.workspacenamespace_update(this.__wbg_ptr, id, ptr0, len0, ptr1, len1);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
}
if (Symbol.dispose) WorkspaceNamespace.prototype[Symbol.dispose] = WorkspaceNamespace.prototype.free;

/**
 * @param {Uint8Array} ciphertext
 * @param {Uint8Array} recipient_sk
 * @param {Uint8Array} sender_pk
 * @returns {Uint8Array}
 */
export function decrypt(ciphertext, recipient_sk, sender_pk) {
    const ptr0 = passArray8ToWasm0(ciphertext, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(recipient_sk, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ptr2 = passArray8ToWasm0(sender_pk, wasm.__wbindgen_malloc);
    const len2 = WASM_VECTOR_LEN;
    const ret = wasm.decrypt(ptr0, len0, ptr1, len1, ptr2, len2);
    if (ret[3]) {
        throw takeFromExternrefTable0(ret[2]);
    }
    var v4 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    return v4;
}

/**
 * @param {Uint8Array} plaintext
 * @param {Uint8Array} sender_sk
 * @param {Uint8Array} recipient_pk
 * @returns {Uint8Array}
 */
export function encrypt(plaintext, sender_sk, recipient_pk) {
    const ptr0 = passArray8ToWasm0(plaintext, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(sender_sk, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ptr2 = passArray8ToWasm0(recipient_pk, wasm.__wbindgen_malloc);
    const len2 = WASM_VECTOR_LEN;
    const ret = wasm.encrypt(ptr0, len0, ptr1, len1, ptr2, len2);
    if (ret[3]) {
        throw takeFromExternrefTable0(ret[2]);
    }
    var v4 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    return v4;
}

export function init() {
    wasm.init();
}

/**
 * @param {string} level
 */
export function set_log_level_from_str(level) {
    const ptr0 = passStringToWasm0(level, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    wasm.set_log_level_from_str(ptr0, len0);
}
function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg___wbindgen_boolean_get_2304fb8c853028c8: function(arg0) {
            const v = arg0;
            const ret = typeof(v) === 'boolean' ? v : undefined;
            return isLikeNone(ret) ? 0xFFFFFF : ret ? 1 : 0;
        },
        __wbg___wbindgen_debug_string_edece8177ad01481: function(arg0, arg1) {
            const ret = debugString(arg1);
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_is_function_5cd60d5cf78b4eef: function(arg0) {
            const ret = typeof(arg0) === 'function';
            return ret;
        },
        __wbg___wbindgen_is_null_2042690d351e14f0: function(arg0) {
            const ret = arg0 === null;
            return ret;
        },
        __wbg___wbindgen_is_object_b4593df85baada48: function(arg0) {
            const val = arg0;
            const ret = typeof(val) === 'object' && val !== null;
            return ret;
        },
        __wbg___wbindgen_is_string_dde0fd9020db4434: function(arg0) {
            const ret = typeof(arg0) === 'string';
            return ret;
        },
        __wbg___wbindgen_is_undefined_35bb9f4c7fd651d5: function(arg0) {
            const ret = arg0 === undefined;
            return ret;
        },
        __wbg___wbindgen_string_get_d109740c0d18f4d7: function(arg0, arg1) {
            const obj = arg1;
            const ret = typeof(obj) === 'string' ? obj : undefined;
            var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            var len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_throw_9c31b086c2b26051: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbg__wbg_cb_unref_3fa391f3fcdb55f8: function(arg0) {
            arg0._wbg_cb_unref();
        },
        __wbg_acquire_lock_immediate_71820c0d3c8f1071: function(arg0, arg1, arg2, arg3, arg4, arg5) {
            const ret = acquire_lock_immediate(getStringFromWasm0(arg0, arg1), getStringFromWasm0(arg2, arg3), arg4, arg5);
            return ret;
        },
        __wbg_acquire_web_lock_25fd29220bdfc055: function(arg0, arg1, arg2, arg3, arg4, arg5) {
            acquire_web_lock(getStringFromWasm0(arg0, arg1), getStringFromWasm0(arg2, arg3), arg4, arg5);
        },
        __wbg_addEventListener_aedacff123afaebd: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            arg0.addEventListener(getStringFromWasm0(arg1, arg2), arg3);
        }, arguments); },
        __wbg_addIceCandidate_8c940af93aa89e1b: function(arg0, arg1) {
            const ret = arg0.addIceCandidate(arg1);
            return ret;
        },
        __wbg_arrayBuffer_0ad0e21451bc9ea0: function(arg0) {
            const ret = arg0.arrayBuffer();
            return ret;
        },
        __wbg_call_084ee3e860ee9f92: function() { return handleError(function (arg0, arg1, arg2, arg3, arg4) {
            const ret = arg0.call(arg1, arg2, arg3, arg4);
            return ret;
        }, arguments); },
        __wbg_call_13665d9f14390edc: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.call(arg1);
            return ret;
        }, arguments); },
        __wbg_call_dfde26266607c996: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.call(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_call_faa0a261f288f846: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            const ret = arg0.call(arg1, arg2, arg3);
            return ret;
        }, arguments); },
        __wbg_candidate_6982144c4b510573: function(arg0) {
            const ret = arg0.candidate;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_candidate_8f84c08a263b1e2d: function(arg0, arg1) {
            const ret = arg1.candidate;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_channel_5307af3b9865026c: function(arg0) {
            const ret = arg0.channel;
            return ret;
        },
        __wbg_clearTimeout_8f9b98f059a1f7a3: function(arg0, arg1) {
            arg0.clearTimeout(arg1);
        },
        __wbg_close_3b4a9a43141c17f7: function(arg0) {
            const ret = arg0.close();
            return ret;
        },
        __wbg_close_e323e9eee669c291: function() { return handleError(function (arg0) {
            arg0.close();
        }, arguments); },
        __wbg_createAnswer_cae011a73f2c0e6a: function(arg0) {
            const ret = arg0.createAnswer();
            return ret;
        },
        __wbg_createDataChannel_07fccf8feff42bbc: function(arg0, arg1, arg2) {
            const ret = arg0.createDataChannel(getStringFromWasm0(arg1, arg2));
            return ret;
        },
        __wbg_createObjectStore_ce6be0d6715f0760: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.createObjectStore(getStringFromWasm0(arg1, arg2));
            return ret;
        }, arguments); },
        __wbg_createOffer_c5465391a526e12d: function(arg0) {
            const ret = arg0.createOffer();
            return ret;
        },
        __wbg_createWritable_7123e728f1062cb0: function(arg0) {
            const ret = arg0.createWritable();
            return ret;
        },
        __wbg_crypto_38df2bab126b63dc: function(arg0) {
            const ret = arg0.crypto;
            return ret;
        },
        __wbg_data_5fc79a19e47d1531: function(arg0) {
            const ret = arg0.data;
            return ret;
        },
        __wbg_debug_f6bcfe64fbe89f8c: function(arg0, arg1) {
            console.debug(getStringFromWasm0(arg0, arg1));
        },
        __wbg_delete_bc03f88e7f14db56: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.delete(arg1);
            return ret;
        }, arguments); },
        __wbg_entries_8db5a11d802d4c53: function(arg0) {
            const ret = arg0.entries();
            return ret;
        },
        __wbg_error_4ce7a41d31c7b468: function(arg0, arg1) {
            console.error(getStringFromWasm0(arg0, arg1));
        },
        __wbg_error_a6fa202b58aa1cd3: function(arg0, arg1) {
            let deferred0_0;
            let deferred0_1;
            try {
                deferred0_0 = arg0;
                deferred0_1 = arg1;
                console.error(getStringFromWasm0(arg0, arg1));
            } finally {
                wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
            }
        },
        __wbg_fetch_47ebc0e53aa08033: function(arg0, arg1) {
            const ret = arg0.fetch(arg1);
            return ret;
        },
        __wbg_getDirectoryHandle_ad42f2fe4fcbaa50: function(arg0, arg1, arg2, arg3) {
            const ret = arg0.getDirectoryHandle(getStringFromWasm0(arg1, arg2), arg3);
            return ret;
        },
        __wbg_getDirectory_1992c7a67af9adfe: function(arg0) {
            const ret = arg0.getDirectory();
            return ret;
        },
        __wbg_getFileHandle_4b28d8702a1efafd: function(arg0, arg1, arg2, arg3) {
            const ret = arg0.getFileHandle(getStringFromWasm0(arg1, arg2), arg3);
            return ret;
        },
        __wbg_getFile_48fd884df3b8ca4f: function(arg0) {
            const ret = arg0.getFile();
            return ret;
        },
        __wbg_getRandomValues_3f44b700395062e5: function() { return handleError(function (arg0, arg1) {
            globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
        }, arguments); },
        __wbg_getRandomValues_c44a50d8cfdaebeb: function() { return handleError(function (arg0, arg1) {
            arg0.getRandomValues(arg1);
        }, arguments); },
        __wbg_get_b6f278d067d9edad: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.get(arg1);
            return ret;
        }, arguments); },
        __wbg_get_dcf82ab8aad1a593: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.get(arg0, arg1);
            return ret;
        }, arguments); },
        __wbg_indexedDB_cbfeacc981615a77: function() { return handleError(function (arg0) {
            const ret = arg0.indexedDB;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        }, arguments); },
        __wbg_instanceof_ArrayBuffer_53db37b06f6b9afe: function(arg0) {
            let result;
            try {
                result = arg0 instanceof ArrayBuffer;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_IdbOpenDbRequest_2cce7fd687448f0f: function(arg0) {
            let result;
            try {
                result = arg0 instanceof IDBOpenDBRequest;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_IdbRequest_eef501cff5d0b7c1: function(arg0) {
            let result;
            try {
                result = arg0 instanceof IDBRequest;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Promise_09012cfa9708520a: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Promise;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Response_ecfc823e8fb354e2: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Response;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_RtcSessionDescription_80df515cc5fec942: function(arg0) {
            let result;
            try {
                result = arg0 instanceof RTCSessionDescription;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_WebSocket_5b52be0d691ebab1: function(arg0) {
            let result;
            try {
                result = arg0 instanceof WebSocket;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Window_faa5cf994f49cca7: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Window;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_is_032c49d03f47f420: function(arg0, arg1) {
            const ret = Object.is(arg0, arg1);
            return ret;
        },
        __wbg_length_2591a0f4f659a55c: function(arg0) {
            const ret = arg0.length;
            return ret;
        },
        __wbg_length_56fcd3e2b7e0299d: function(arg0) {
            const ret = arg0.length;
            return ret;
        },
        __wbg_log_168b1fa9576f2040: function(arg0, arg1) {
            console.log(getStringFromWasm0(arg0, arg1));
        },
        __wbg_msCrypto_bd5a034af96bcba6: function(arg0) {
            const ret = arg0.msCrypto;
            return ret;
        },
        __wbg_navigator_3db7ba343e05d4d1: function(arg0) {
            const ret = arg0.navigator;
            return ret;
        },
        __wbg_new_02adbe70c44f51bf: function() { return handleError(function (arg0, arg1) {
            const ret = new BroadcastChannel(getStringFromWasm0(arg0, arg1));
            return ret;
        }, arguments); },
        __wbg_new_02d162bc6cf02f60: function() {
            const ret = new Object();
            return ret;
        },
        __wbg_new_0_2722fcdb71a888a6: function() {
            const ret = new Date();
            return ret;
        },
        __wbg_new_227d7c05414eb861: function() {
            const ret = new Error();
            return ret;
        },
        __wbg_new_310879b66b6e95e1: function() {
            const ret = new Array();
            return ret;
        },
        __wbg_new_7ddec6de44ff8f5d: function(arg0) {
            const ret = new Uint8Array(arg0);
            return ret;
        },
        __wbg_new_b1280f836646084c: function() { return handleError(function (arg0, arg1) {
            const ret = new WebSocket(getStringFromWasm0(arg0, arg1));
            return ret;
        }, arguments); },
        __wbg_new_e68d86570f2af015: function() { return handleError(function (arg0) {
            const ret = new RTCIceCandidate(arg0);
            return ret;
        }, arguments); },
        __wbg_new_from_slice_269e35316ed2d061: function(arg0, arg1) {
            const ret = new Uint8Array(getArrayU8FromWasm0(arg0, arg1));
            return ret;
        },
        __wbg_new_typed_c072c4ce9a2a0cdf: function(arg0, arg1) {
            try {
                var state0 = {a: arg0, b: arg1};
                var cb0 = (arg0, arg1) => {
                    const a = state0.a;
                    state0.a = 0;
                    try {
                        return wasm_bindgen__convert__closures_____invoke__h6742839cb717cdad(a, state0.b, arg0, arg1);
                    } finally {
                        state0.a = a;
                    }
                };
                const ret = new Promise(cb0);
                return ret;
            } finally {
                state0.a = 0;
            }
        },
        __wbg_new_with_configuration_72f2daa691f2c34f: function() { return handleError(function (arg0) {
            const ret = new RTCPeerConnection(arg0);
            return ret;
        }, arguments); },
        __wbg_new_with_length_99887c91eae4abab: function(arg0) {
            const ret = new Uint8Array(arg0 >>> 0);
            return ret;
        },
        __wbg_new_with_str_and_init_ffe9977c986ea039: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = new Request(getStringFromWasm0(arg0, arg1), arg2);
            return ret;
        }, arguments); },
        __wbg_node_84ea875411254db1: function(arg0) {
            const ret = arg0.node;
            return ret;
        },
        __wbg_now_3cd905700d21a70b: function(arg0) {
            const ret = arg0.now();
            return ret;
        },
        __wbg_now_81363d44c96dd239: function() {
            const ret = Date.now();
            return ret;
        },
        __wbg_now_e7c6795a7f81e10f: function(arg0) {
            const ret = arg0.now();
            return ret;
        },
        __wbg_objectStore_b28adb984a77902e: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.objectStore(getStringFromWasm0(arg1, arg2));
            return ret;
        }, arguments); },
        __wbg_ok_556a55299dd238ba: function(arg0) {
            const ret = arg0.ok;
            return ret;
        },
        __wbg_open_40ab11cdd8f5ac5a: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            const ret = arg0.open(getStringFromWasm0(arg1, arg2), arg3 >>> 0);
            return ret;
        }, arguments); },
        __wbg_parse_2c1cad6215e84999: function() { return handleError(function (arg0, arg1) {
            const ret = JSON.parse(getStringFromWasm0(arg0, arg1));
            return ret;
        }, arguments); },
        __wbg_performance_3fcf6e32a7e1ed0a: function(arg0) {
            const ret = arg0.performance;
            return ret;
        },
        __wbg_performance_ddd4e7eeef6254f3: function(arg0) {
            const ret = arg0.performance;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_postMessage_748fa9cada6f4dff: function() { return handleError(function (arg0, arg1) {
            arg0.postMessage(arg1);
        }, arguments); },
        __wbg_process_44c7a14e11e9f69e: function(arg0) {
            const ret = arg0.process;
            return ret;
        },
        __wbg_prototypesetcall_5f9bdc8d75e07276: function(arg0, arg1, arg2) {
            Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), arg2);
        },
        __wbg_push_b77c476b01548d0a: function(arg0, arg1) {
            const ret = arg0.push(arg1);
            return ret;
        },
        __wbg_put_ad17713e2c2f12fa: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.put(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_queueMicrotask_78d584b53af520f5: function(arg0) {
            const ret = arg0.queueMicrotask;
            return ret;
        },
        __wbg_queueMicrotask_b39ea83c7f01971a: function(arg0) {
            queueMicrotask(arg0);
        },
        __wbg_randomFillSync_6c25eac9869eb53c: function() { return handleError(function (arg0, arg1) {
            arg0.randomFillSync(arg1);
        }, arguments); },
        __wbg_readyState_a1a00cc8898812ac: function(arg0) {
            const ret = arg0.readyState;
            return ret;
        },
        __wbg_release_web_lock_31ca3d431bb73de0: function(arg0, arg1) {
            release_web_lock(getStringFromWasm0(arg0, arg1));
        },
        __wbg_removeEntry_871c09eb04b32892: function(arg0, arg1, arg2) {
            const ret = arg0.removeEntry(getStringFromWasm0(arg1, arg2));
            return ret;
        },
        __wbg_require_b4edbdcf3e2a1ef0: function() { return handleError(function () {
            const ret = module.require;
            return ret;
        }, arguments); },
        __wbg_resolve_d17db9352f5a220e: function(arg0) {
            const ret = Promise.resolve(arg0);
            return ret;
        },
        __wbg_result_c4cb33cd39c97cac: function() { return handleError(function (arg0) {
            const ret = arg0.result;
            return ret;
        }, arguments); },
        __wbg_sdp_0b1a8ca6a0098a74: function(arg0, arg1) {
            const ret = arg1.sdp;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_send_b9d998a91cbc8429: function() { return handleError(function (arg0, arg1, arg2) {
            arg0.send(getArrayU8FromWasm0(arg1, arg2));
        }, arguments); },
        __wbg_setLocalDescription_722a42b3d6405004: function(arg0, arg1) {
            const ret = arg0.setLocalDescription(arg1);
            return ret;
        },
        __wbg_setRemoteDescription_395f25223a37bd73: function(arg0, arg1) {
            const ret = arg0.setRemoteDescription(arg1);
            return ret;
        },
        __wbg_setTimeout_4a8f96a1b4261aee: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.setTimeout(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_set_24d0fa9e104112f9: function(arg0, arg1, arg2) {
            arg0.set(getArrayU8FromWasm0(arg1, arg2));
        },
        __wbg_set_a0e911be3da02782: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = Reflect.set(arg0, arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_set_binaryType_5c0002dfcf194934: function(arg0, arg1) {
            arg0.binaryType = __wbindgen_enum_BinaryType[arg1];
        },
        __wbg_set_candidate_8722578330810fe7: function(arg0, arg1, arg2) {
            arg0.candidate = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_create_5cb26b898b874123: function(arg0, arg1) {
            arg0.create = arg1 !== 0;
        },
        __wbg_set_create_d6d52ca457b8e339: function(arg0, arg1) {
            arg0.create = arg1 !== 0;
        },
        __wbg_set_ice_servers_49ee84e1a8d71724: function(arg0, arg1) {
            arg0.iceServers = arg1;
        },
        __wbg_set_method_4d69a1a7e34c0aca: function(arg0, arg1, arg2) {
            arg0.method = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_onbeforeunload_f8978046473c05ae: function(arg0, arg1) {
            arg0.onbeforeunload = arg1;
        },
        __wbg_set_onclose_3121e15055418a37: function(arg0, arg1) {
            arg0.onclose = arg1;
        },
        __wbg_set_ondatachannel_8f2f10b5f6430352: function(arg0, arg1) {
            arg0.ondatachannel = arg1;
        },
        __wbg_set_onerror_38740b892815eedc: function(arg0, arg1) {
            arg0.onerror = arg1;
        },
        __wbg_set_onicecandidate_6f8679d6f6e68b8d: function(arg0, arg1) {
            arg0.onicecandidate = arg1;
        },
        __wbg_set_onmessage_048312f6b7a5ce38: function(arg0, arg1) {
            arg0.onmessage = arg1;
        },
        __wbg_set_onmessage_9eb2cf76e70783ad: function(arg0, arg1) {
            arg0.onmessage = arg1;
        },
        __wbg_set_onmessage_e6eeb89e96cab49a: function(arg0, arg1) {
            arg0.onmessage = arg1;
        },
        __wbg_set_onopen_6f3fc5e2ad3144f1: function(arg0, arg1) {
            arg0.onopen = arg1;
        },
        __wbg_set_onsuccess_b556141053d02ea7: function(arg0, arg1) {
            arg0.onsuccess = arg1;
        },
        __wbg_set_onupgradeneeded_f885fa17614acd2b: function(arg0, arg1) {
            arg0.onupgradeneeded = arg1;
        },
        __wbg_set_sdp_0dca6634c7f0baa8: function(arg0, arg1, arg2) {
            arg0.sdp = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_sdp_m_line_index_84895cde791b938e: function(arg0, arg1) {
            arg0.sdpMLineIndex = arg1 === 0xFFFFFF ? undefined : arg1;
        },
        __wbg_set_type_da7bc1c21cfe0a36: function(arg0, arg1) {
            arg0.type = __wbindgen_enum_RtcSdpType[arg1];
        },
        __wbg_slice_3f9ca2832ef75a48: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.slice(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_stack_3b0d974bbf31e44f: function(arg0, arg1) {
            const ret = arg1.stack;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_static_accessor_GLOBAL_THIS_02344c9b09eb08a9: function() {
            const ret = typeof globalThis === 'undefined' ? null : globalThis;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_GLOBAL_ac6d4ac874d5cd54: function() {
            const ret = typeof global === 'undefined' ? null : global;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_SELF_9b2406c23aeb2023: function() {
            const ret = typeof self === 'undefined' ? null : self;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_WINDOW_b34d2126934e16ba: function() {
            const ret = typeof window === 'undefined' ? null : window;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_status_0853c9f5752c7ee2: function(arg0) {
            const ret = arg0.status;
            return ret;
        },
        __wbg_storage_437513bfd60306b6: function(arg0) {
            const ret = arg0.storage;
            return ret;
        },
        __wbg_stringify_ef0c105b1ccc3849: function() { return handleError(function (arg0) {
            const ret = JSON.stringify(arg0);
            return ret;
        }, arguments); },
        __wbg_subarray_7c6a0da8f3b4a1ba: function(arg0, arg1, arg2) {
            const ret = arg0.subarray(arg1 >>> 0, arg2 >>> 0);
            return ret;
        },
        __wbg_target_84e05e84ffc12989: function(arg0) {
            const ret = arg0.target;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_text_99930d92d5f1b540: function() { return handleError(function (arg0) {
            const ret = arg0.text();
            return ret;
        }, arguments); },
        __wbg_then_837494e384b37459: function(arg0, arg1) {
            const ret = arg0.then(arg1);
            return ret;
        },
        __wbg_then_87e0b598b245104b: function(arg0, arg1, arg2) {
            const ret = arg0.then(arg1, arg2);
            return ret;
        },
        __wbg_then_bd927500e8905df2: function(arg0, arg1, arg2) {
            const ret = arg0.then(arg1, arg2);
            return ret;
        },
        __wbg_trace_822dbcb10dfd03eb: function(arg0, arg1) {
            console.trace(getStringFromWasm0(arg0, arg1));
        },
        __wbg_transaction_b7261fed68fa4264: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            const ret = arg0.transaction(getStringFromWasm0(arg1, arg2), __wbindgen_enum_IdbTransactionMode[arg3]);
            return ret;
        }, arguments); },
        __wbg_vaultsyncruntime_new: function(arg0) {
            const ret = VaultSyncRuntime.__wrap(arg0);
            return ret;
        },
        __wbg_versions_276b2795b1c6a219: function(arg0) {
            const ret = arg0.versions;
            return ret;
        },
        __wbg_warn_ed320d51da79dd3d: function(arg0, arg1) {
            console.warn(getStringFromWasm0(arg0, arg1));
        },
        __wbg_write_4a589e6ce69a6253: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.write(arg1);
            return ret;
        }, arguments); },
        __wbindgen_cast_0000000000000001: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [Externref], shim_idx: 1062, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df);
            return ret;
        },
        __wbindgen_cast_0000000000000002: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [Externref], shim_idx: 1353, ret: Result(Unit), inner_ret: Some(Result(Unit)) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h685410aed2fde3f1);
            return ret;
        },
        __wbindgen_cast_0000000000000003: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("Event")], shim_idx: 1060, ret: Unit, inner_ret: Some(Unit) }, mutable: false }) -> Externref`.
            const ret = makeClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h6fb81e698e30f778);
            return ret;
        },
        __wbindgen_cast_0000000000000004: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("MessageEvent")], shim_idx: 1062, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df_3);
            return ret;
        },
        __wbindgen_cast_0000000000000005: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("MessageEvent")], shim_idx: 1093, ret: Unit, inner_ret: Some(Unit) }, mutable: false }) -> Externref`.
            const ret = makeClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0);
            return ret;
        },
        __wbindgen_cast_0000000000000006: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("RTCDataChannelEvent")], shim_idx: 1093, ret: Unit, inner_ret: Some(Unit) }, mutable: false }) -> Externref`.
            const ret = makeClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_5);
            return ret;
        },
        __wbindgen_cast_0000000000000007: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("RTCPeerConnectionIceEvent")], shim_idx: 1093, ret: Unit, inner_ret: Some(Unit) }, mutable: false }) -> Externref`.
            const ret = makeClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_6);
            return ret;
        },
        __wbindgen_cast_0000000000000008: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [], shim_idx: 1272, ret: Unit, inner_ret: Some(Unit) }, mutable: false }) -> Externref`.
            const ret = makeClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__h5a816e701a8600f2);
            return ret;
        },
        __wbindgen_cast_0000000000000009: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [], shim_idx: 1274, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen__convert__closures_____invoke__hd15eeae72bcd25a2);
            return ret;
        },
        __wbindgen_cast_000000000000000a: function(arg0) {
            // Cast intrinsic for `F64 -> Externref`.
            const ret = arg0;
            return ret;
        },
        __wbindgen_cast_000000000000000b: function(arg0, arg1) {
            // Cast intrinsic for `Ref(Slice(U8)) -> NamedExternref("Uint8Array")`.
            const ret = getArrayU8FromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_cast_000000000000000c: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_init_externref_table: function() {
            const table = wasm.__wbindgen_externrefs;
            const offset = table.grow(4);
            table.set(0, undefined);
            table.set(offset + 0, undefined);
            table.set(offset + 1, null);
            table.set(offset + 2, true);
            table.set(offset + 3, false);
        },
    };
    return {
        __proto__: null,
        "./vaultsync_wasm_bg.js": import0,
    };
}

function wasm_bindgen__convert__closures_____invoke__h5a816e701a8600f2(arg0, arg1) {
    wasm.wasm_bindgen__convert__closures_____invoke__h5a816e701a8600f2(arg0, arg1);
}

function wasm_bindgen__convert__closures_____invoke__hd15eeae72bcd25a2(arg0, arg1) {
    wasm.wasm_bindgen__convert__closures_____invoke__hd15eeae72bcd25a2(arg0, arg1);
}

function wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h6fb81e698e30f778(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h6fb81e698e30f778(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df_3(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df_3(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_5(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_5(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_6(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_6(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h685410aed2fde3f1(arg0, arg1, arg2) {
    const ret = wasm.wasm_bindgen__convert__closures_____invoke__h685410aed2fde3f1(arg0, arg1, arg2);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

function wasm_bindgen__convert__closures_____invoke__h6742839cb717cdad(arg0, arg1, arg2, arg3) {
    wasm.wasm_bindgen__convert__closures_____invoke__h6742839cb717cdad(arg0, arg1, arg2, arg3);
}


const __wbindgen_enum_BinaryType = ["blob", "arraybuffer"];


const __wbindgen_enum_IdbTransactionMode = ["readonly", "readwrite", "versionchange", "readwriteflush", "cleanup"];


const __wbindgen_enum_RtcSdpType = ["offer", "pranswer", "answer", "rollback"];
const EventBusProxyFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_eventbusproxy_free(ptr, 1));
const PresenceManagerFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_presencemanager_free(ptr, 1));
const ReplicationNamespaceFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_replicationnamespace_free(ptr, 1));
const VaultSyncRuntimeFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_vaultsyncruntime_free(ptr, 1));
const WasmIPCFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmipc_free(ptr, 1));
const WasmSubscriptionHandleFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmsubscriptionhandle_free(ptr, 1));
const WorkingSetsNamespaceFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_workingsetsnamespace_free(ptr, 1));
const WorkspaceNamespaceFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_workspacenamespace_free(ptr, 1));

function addToExternrefTable0(obj) {
    const idx = wasm.__externref_table_alloc();
    wasm.__wbindgen_externrefs.set(idx, obj);
    return idx;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
}

const CLOSURE_DTORS = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(state => wasm.__wbindgen_destroy_closure(state.a, state.b));

function debugString(val) {
    // primitive types
    const type = typeof val;
    if (type == 'number' || type == 'boolean' || val == null) {
        return  `${val}`;
    }
    if (type == 'string') {
        return `"${val}"`;
    }
    if (type == 'symbol') {
        const description = val.description;
        if (description == null) {
            return 'Symbol';
        } else {
            return `Symbol(${description})`;
        }
    }
    if (type == 'function') {
        const name = val.name;
        if (typeof name == 'string' && name.length > 0) {
            return `Function(${name})`;
        } else {
            return 'Function';
        }
    }
    // objects
    if (Array.isArray(val)) {
        const length = val.length;
        let debug = '[';
        if (length > 0) {
            debug += debugString(val[0]);
        }
        for(let i = 1; i < length; i++) {
            debug += ', ' + debugString(val[i]);
        }
        debug += ']';
        return debug;
    }
    // Test for built-in
    const builtInMatches = /\[object ([^\]]+)\]/.exec(toString.call(val));
    let className;
    if (builtInMatches && builtInMatches.length > 1) {
        className = builtInMatches[1];
    } else {
        // Failed to match the standard '[object ClassName]'
        return toString.call(val);
    }
    if (className == 'Object') {
        // we're a user defined class or Object
        // JSON.stringify avoids problems with cycles, and is generally much
        // easier than looping through ownProperties of `val`.
        try {
            return 'Object(' + JSON.stringify(val) + ')';
        } catch (_) {
            return 'Object';
        }
    }
    // errors
    if (val instanceof Error) {
        return `${val.name}: ${val.message}\n${val.stack}`;
    }
    // TODO we could test for more things here, like `Set`s and `Map`s.
    return className;
}

function getArrayJsValueFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    const mem = getDataViewMemory0();
    const result = [];
    for (let i = ptr; i < ptr + 4 * len; i += 4) {
        result.push(wasm.__wbindgen_externrefs.get(mem.getUint32(i, true)));
    }
    wasm.__externref_drop_slice(ptr, len);
    return result;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}

let cachedDataViewMemory0 = null;
function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

function getStringFromWasm0(ptr, len) {
    return decodeText(ptr >>> 0, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        const idx = addToExternrefTable0(e);
        wasm.__wbindgen_exn_store(idx);
    }
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

function makeClosure(arg0, arg1, f) {
    const state = { a: arg0, b: arg1, cnt: 1 };
    const real = (...args) => {

        // First up with a closure we increment the internal reference
        // count. This ensures that the Rust closure environment won't
        // be deallocated while we're invoking it.
        state.cnt++;
        try {
            return f(state.a, state.b, ...args);
        } finally {
            real._wbg_cb_unref();
        }
    };
    real._wbg_cb_unref = () => {
        if (--state.cnt === 0) {
            wasm.__wbindgen_destroy_closure(state.a, state.b);
            state.a = 0;
            CLOSURE_DTORS.unregister(state);
        }
    };
    CLOSURE_DTORS.register(real, state, state);
    return real;
}

function makeMutClosure(arg0, arg1, f) {
    const state = { a: arg0, b: arg1, cnt: 1 };
    const real = (...args) => {

        // First up with a closure we increment the internal reference
        // count. This ensures that the Rust closure environment won't
        // be deallocated while we're invoking it.
        state.cnt++;
        const a = state.a;
        state.a = 0;
        try {
            return f(a, state.b, ...args);
        } finally {
            state.a = a;
            real._wbg_cb_unref();
        }
    };
    real._wbg_cb_unref = () => {
        if (--state.cnt === 0) {
            wasm.__wbindgen_destroy_closure(state.a, state.b);
            state.a = 0;
            CLOSURE_DTORS.unregister(state);
        }
    };
    CLOSURE_DTORS.register(real, state, state);
    return real;
}

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8ArrayMemory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_externrefs.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    };
}

let WASM_VECTOR_LEN = 0;

let wasmModule, wasmInstance, wasm;
function __wbg_finalize_init(instance, module) {
    wasmInstance = instance;
    wasm = instance.exports;
    wasmModule = module;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;
    wasm.__wbindgen_start();
    return wasm;
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = module.ok && expectedResponseType(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else { throw e; }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }

    function expectedResponseType(type) {
        switch (type) {
            case 'basic': case 'cors': case 'default': return true;
        }
        return false;
    }
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (module !== undefined) {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (module_or_path !== undefined) {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (module_or_path === undefined) {
        module_or_path = new URL('vaultsync_wasm_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync, __wbg_init as default };
