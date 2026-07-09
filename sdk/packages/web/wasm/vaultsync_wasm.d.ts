/* tslint:disable */
/* eslint-disable */

export enum ClientEvent {
    StatusDirty = 0,
}

export class EventBusProxy {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    subscriberCount(): number;
}

/**
 * Manages multi-tab presence awareness via BroadcastChannel.
 *
 * Each tab announces join/leave/heartbeat so peers know who is active.
 * Exported to JavaScript via `#[wasm_bindgen]`.
 */
export class PresenceManager {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns a JSON string of active peers: `{"replica_id": last_seen_ms, ...}`.
     */
    activePeers(): string;
    /**
     * Create and start announcing presence on a dedicated BroadcastChannel.
     * The channel name is `"vaultsync-presence-{namespace}"`.
     */
    constructor(namespace: string, replica_id: string);
    /**
     * Number of visible peers (excluding self).
     */
    peer_count(): number;
}

export class ReplicationNamespace {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    clearPending(): void;
    pendingCount(): number;
    predictNext(limit: number): string;
}

export class VaultSyncRuntime {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    active_key_version(): bigint;
    /**
     * Phase 4f: Check if mirror should promote to leader (leader heartbeat timeout).
     * Returns true if leader is gone and JS should reinitialize with new_with_coordinator().
     */
    checkMirrorPromotion(): boolean;
    /**
     * Runs compaction on the given namespace, returns JSON stats.
     */
    compactNamespace(namespace: string): Promise<string>;
    define_schema(doc_id: string, schema_json: string): Promise<void>;
    delete(doc_id: string, record_id: string): Promise<void>;
    /**
     * Returns the event bus for publishing/subscribing to engine events.
     */
    events(): EventBusProxy;
    find(doc_id: string): Promise<Array<any>>;
    fire_subscription(doc_id: string, record_id: string): Promise<void>;
    /**
     * Phase 5: Flush pending uploads through the UploadScheduler.
     * Drains pending mutations from the debounced queue and processes them
     * through VaultSyncClient. Returns JSON with processed count and status.
     * Call this periodically (e.g., every 100ms via JS setInterval).
     */
    flushPendingUploads(): Promise<string>;
    get(doc_id: string, record_id: string): Promise<any>;
    /**
     * Leader processes a command from a follower: executes the mutation through the normal
     * VaultSyncClient pipeline, then broadcasts invalidation to all tabs.
     */
    handle_command(verb: string, doc_id: string, record_id: string, json: string): Promise<void>;
    insert(doc_id: string, record_id: string, json: string): Promise<void>;
    is_leader(): boolean;
    /**
     * Phase 6: Returns last compaction duration in ms, or 0 if never run.
     */
    lastCompactionMs(): bigint;
    list_key_versions(): any;
    /**
     * Returns a JSON snapshot of all metrics counters.
     * Phase 4: Returns mirror metrics in follower mode.
     */
    metricsSnapshot(): string;
    static new(namespace: string, replica_id: string, db_name?: string | null, storage_backend?: string | null): Promise<VaultSyncRuntime>;
    static new_with_coordinator(namespace: string, replica_id: string, coordinator_url: string, auth_token?: string | null, db_name?: string | null, storage_backend?: string | null): Promise<VaultSyncRuntime>;
    on_event(callback: Function): void;
    /**
     * Returns a clone of the PresenceManager if available.
     */
    presence(): PresenceManager | undefined;
    prune_key_versions(keep_versions: number): void;
    /**
     * Returns the replication namespace proxy.
     */
    replication(): ReplicationNamespace;
    /**
     * Runs the resource manager sweep (recompute access scores, promote/demote tiers).
     * Returns JSON with tier byte counts, promotions, demotions, eviction candidates.
     */
    resourceSweep(): Promise<string>;
    rotate_keys(): Promise<any>;
    /**
     * Runs lifecycle (tombstone cleanup) on the given namespace, returns JSON stats.
     */
    runLifecycle(namespace: string): Promise<string>;
    /**
     * Engine V2: expose Runtime + PersistenceEngine health stats to JS
     */
    runtime_storage_stats(): any;
    shutdown(): Promise<void>;
    /**
     * Returns JSON with storage statistics (page count, segment state, etc.).
     */
    storageStats(): Promise<string>;
    subscribe(doc_id: string, callback: Function): WasmSubscriptionHandle;
    sync_status(): Promise<any>;
    /**
     * Phase 6: Try auto-compaction via CompactionScheduler.
     * Returns JSON with compaction stats, or null if no compaction was needed.
     * JS should call this periodically (e.g., every 30s via setInterval).
     */
    tryCompact(): Promise<any>;
    unsubscribe(handle: WasmSubscriptionHandle): void;
    update(doc_id: string, record_id: string, json: string): Promise<void>;
    /**
     * Returns the working sets namespace proxy for CRUD operations.
     */
    workingSets(): WorkingSetsNamespace;
    /**
     * Returns the workspace namespace proxy for CRUD operations.
     */
    workspace(): WorkspaceNamespace;
}

export class WasmIPC {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    static new(channel_name: string): WasmIPC;
    /**
     * Create WasmIPC from an existing BroadcastChannel (used by client integration)
     */
    static new_with_channel(channel: BroadcastChannel): WasmIPC;
    on_message(callback: Function): void;
    receive(): string;
    send(msg: string): void;
}

export class WasmSubscriptionHandle {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(client: VaultSyncRuntime): void;
}

export class WorkingSetsNamespace {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    clearActive(): void;
    create(name: string, filter_json: string, workspace_id?: bigint | null): bigint;
    delete(id: bigint): boolean;
    getActive(): any;
    list(): string;
    setActive(id: bigint): boolean;
}

export class WorkspaceNamespace {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    create(name: string, namespace: string, schema_json: string, retention_json: string): bigint;
    delete(id: bigint): boolean;
    get(id: bigint): string;
    list(): string;
    update(id: bigint, name?: string | null, retention_json?: string | null): boolean;
}

export function decrypt(ciphertext: Uint8Array, recipient_sk: Uint8Array, sender_pk: Uint8Array): Uint8Array;

export function encrypt(plaintext: Uint8Array, sender_sk: Uint8Array, recipient_pk: Uint8Array): Uint8Array;

export function init(): void;

export function set_log_level_from_str(level: string): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_presencemanager_free: (a: number, b: number) => void;
    readonly presencemanager_activePeers: (a: number) => [number, number];
    readonly presencemanager_new: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly presencemanager_peer_count: (a: number) => number;
    readonly init: () => void;
    readonly set_log_level_from_str: (a: number, b: number) => void;
    readonly decrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly encrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly __wbg_eventbusproxy_free: (a: number, b: number) => void;
    readonly __wbg_replicationnamespace_free: (a: number, b: number) => void;
    readonly __wbg_vaultsyncruntime_free: (a: number, b: number) => void;
    readonly __wbg_wasmipc_free: (a: number, b: number) => void;
    readonly __wbg_wasmsubscriptionhandle_free: (a: number, b: number) => void;
    readonly __wbg_workingsetsnamespace_free: (a: number, b: number) => void;
    readonly __wbg_workspacenamespace_free: (a: number, b: number) => void;
    readonly eventbusproxy_subscriberCount: (a: number) => number;
    readonly replicationnamespace_clearPending: (a: number) => void;
    readonly replicationnamespace_pendingCount: (a: number) => number;
    readonly replicationnamespace_predictNext: (a: number, b: number) => [number, number, number, number];
    readonly vaultsyncruntime_active_key_version: (a: number) => bigint;
    readonly vaultsyncruntime_checkMirrorPromotion: (a: number) => number;
    readonly vaultsyncruntime_compactNamespace: (a: number, b: number, c: number) => any;
    readonly vaultsyncruntime_define_schema: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly vaultsyncruntime_delete: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly vaultsyncruntime_events: (a: number) => number;
    readonly vaultsyncruntime_find: (a: number, b: number, c: number) => any;
    readonly vaultsyncruntime_fire_subscription: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly vaultsyncruntime_flushPendingUploads: (a: number) => any;
    readonly vaultsyncruntime_get: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly vaultsyncruntime_handle_command: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => any;
    readonly vaultsyncruntime_insert: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly vaultsyncruntime_is_leader: (a: number) => number;
    readonly vaultsyncruntime_lastCompactionMs: (a: number) => bigint;
    readonly vaultsyncruntime_list_key_versions: (a: number) => [number, number, number];
    readonly vaultsyncruntime_metricsSnapshot: (a: number) => [number, number];
    readonly vaultsyncruntime_new: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => any;
    readonly vaultsyncruntime_new_with_coordinator: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number, k: number, l: number) => any;
    readonly vaultsyncruntime_on_event: (a: number, b: any) => void;
    readonly vaultsyncruntime_presence: (a: number) => number;
    readonly vaultsyncruntime_prune_key_versions: (a: number, b: number) => [number, number];
    readonly vaultsyncruntime_replication: (a: number) => number;
    readonly vaultsyncruntime_resourceSweep: (a: number) => any;
    readonly vaultsyncruntime_rotate_keys: (a: number) => any;
    readonly vaultsyncruntime_runLifecycle: (a: number, b: number, c: number) => any;
    readonly vaultsyncruntime_runtime_storage_stats: (a: number) => [number, number, number];
    readonly vaultsyncruntime_shutdown: (a: number) => any;
    readonly vaultsyncruntime_storageStats: (a: number) => any;
    readonly vaultsyncruntime_subscribe: (a: number, b: number, c: number, d: any) => number;
    readonly vaultsyncruntime_sync_status: (a: number) => any;
    readonly vaultsyncruntime_tryCompact: (a: number) => any;
    readonly vaultsyncruntime_unsubscribe: (a: number, b: number) => [number, number];
    readonly vaultsyncruntime_update: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly vaultsyncruntime_workingSets: (a: number) => number;
    readonly vaultsyncruntime_workspace: (a: number) => number;
    readonly wasmipc_new: (a: number, b: number) => [number, number, number];
    readonly wasmipc_new_with_channel: (a: any) => number;
    readonly wasmipc_on_message: (a: number, b: any) => void;
    readonly wasmipc_receive: (a: number) => [number, number];
    readonly wasmipc_send: (a: number, b: number, c: number) => [number, number];
    readonly wasmsubscriptionhandle_cancel: (a: number, b: number) => [number, number];
    readonly workingsetsnamespace_clearActive: (a: number) => void;
    readonly workingsetsnamespace_create: (a: number, b: number, c: number, d: number, e: number, f: number, g: bigint) => [bigint, number, number];
    readonly workingsetsnamespace_delete: (a: number, b: bigint) => number;
    readonly workingsetsnamespace_getActive: (a: number) => any;
    readonly workingsetsnamespace_list: (a: number) => [number, number, number, number];
    readonly workingsetsnamespace_setActive: (a: number, b: bigint) => number;
    readonly workspacenamespace_create: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => [bigint, number, number];
    readonly workspacenamespace_delete: (a: number, b: bigint) => number;
    readonly workspacenamespace_get: (a: number, b: bigint) => [number, number, number, number];
    readonly workspacenamespace_list: (a: number) => [number, number, number, number];
    readonly workspacenamespace_update: (a: number, b: bigint, c: number, d: number, e: number, f: number) => [number, number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h685410aed2fde3f1: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h6742839cb717cdad: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h6fb81e698e30f778: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h8fef397f314f78df_3: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_5: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h3f5a6bd03c85dcd0_6: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h5a816e701a8600f2: (a: number, b: number) => void;
    readonly wasm_bindgen__convert__closures_____invoke__hd15eeae72bcd25a2: (a: number, b: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
