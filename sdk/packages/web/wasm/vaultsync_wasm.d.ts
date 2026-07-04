/* tslint:disable */
/* eslint-disable */

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

export class WasmIPC {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    static new(channel_name: string): WasmIPC;
    on_message(callback: Function): void;
    receive(): string;
    send(msg: string): void;
}

export class WasmSubscriptionHandle {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(client: WasmVaultSyncClient): void;
}

export class WasmVaultSyncClient {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    active_key_version(): bigint;
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
    get(doc_id: string, record_id: string): Promise<any>;
    /**
     * Leader processes a command from a follower: executes the mutation through the normal
     * VaultSyncClient pipeline, then broadcasts invalidation to all tabs.
     */
    handle_command(verb: string, doc_id: string, record_id: string, json: string): Promise<void>;
    insert(doc_id: string, record_id: string, json: string): Promise<void>;
    is_leader(): boolean;
    list_key_versions(): any;
    /**
     * Returns a JSON snapshot of all metrics counters.
     */
    metricsSnapshot(): string;
    static new(namespace: string, replica_id: string, db_name?: string | null, storage_backend?: string | null): Promise<WasmVaultSyncClient>;
    static new_with_coordinator(namespace: string, replica_id: string, coordinator_url: string, auth_token?: string | null, db_name?: string | null, storage_backend?: string | null): Promise<WasmVaultSyncClient>;
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
    shutdown(): Promise<void>;
    /**
     * Returns JSON with storage statistics (page count, segment state, etc.).
     */
    storageStats(): Promise<string>;
    subscribe(doc_id: string, callback: Function): WasmSubscriptionHandle;
    sync_status(): Promise<any>;
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
    readonly __wbg_wasmipc_free: (a: number, b: number) => void;
    readonly wasmipc_new: (a: number, b: number) => [number, number, number];
    readonly wasmipc_on_message: (a: number, b: any) => void;
    readonly wasmipc_receive: (a: number) => [number, number];
    readonly wasmipc_send: (a: number, b: number, c: number) => [number, number];
    readonly decrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly encrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly init: () => void;
    readonly set_log_level_from_str: (a: number, b: number) => void;
    readonly __wbg_eventbusproxy_free: (a: number, b: number) => void;
    readonly __wbg_replicationnamespace_free: (a: number, b: number) => void;
    readonly __wbg_wasmsubscriptionhandle_free: (a: number, b: number) => void;
    readonly __wbg_wasmvaultsyncclient_free: (a: number, b: number) => void;
    readonly __wbg_workingsetsnamespace_free: (a: number, b: number) => void;
    readonly __wbg_workspacenamespace_free: (a: number, b: number) => void;
    readonly eventbusproxy_subscriberCount: (a: number) => number;
    readonly replicationnamespace_clearPending: (a: number) => void;
    readonly replicationnamespace_pendingCount: (a: number) => number;
    readonly replicationnamespace_predictNext: (a: number, b: number) => [number, number, number, number];
    readonly wasmsubscriptionhandle_cancel: (a: number, b: number) => [number, number];
    readonly wasmvaultsyncclient_active_key_version: (a: number) => bigint;
    readonly wasmvaultsyncclient_compactNamespace: (a: number, b: number, c: number) => any;
    readonly wasmvaultsyncclient_define_schema: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_delete: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_events: (a: number) => number;
    readonly wasmvaultsyncclient_find: (a: number, b: number, c: number) => any;
    readonly wasmvaultsyncclient_fire_subscription: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_get: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_handle_command: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => any;
    readonly wasmvaultsyncclient_insert: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly wasmvaultsyncclient_is_leader: (a: number) => number;
    readonly wasmvaultsyncclient_list_key_versions: (a: number) => [number, number, number];
    readonly wasmvaultsyncclient_metricsSnapshot: (a: number) => [number, number];
    readonly wasmvaultsyncclient_new: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => any;
    readonly wasmvaultsyncclient_new_with_coordinator: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number, k: number, l: number) => any;
    readonly wasmvaultsyncclient_presence: (a: number) => number;
    readonly wasmvaultsyncclient_prune_key_versions: (a: number, b: number) => [number, number];
    readonly wasmvaultsyncclient_replication: (a: number) => number;
    readonly wasmvaultsyncclient_resourceSweep: (a: number) => any;
    readonly wasmvaultsyncclient_rotate_keys: (a: number) => any;
    readonly wasmvaultsyncclient_runLifecycle: (a: number, b: number, c: number) => any;
    readonly wasmvaultsyncclient_shutdown: (a: number) => any;
    readonly wasmvaultsyncclient_storageStats: (a: number) => any;
    readonly wasmvaultsyncclient_subscribe: (a: number, b: number, c: number, d: any) => number;
    readonly wasmvaultsyncclient_sync_status: (a: number) => any;
    readonly wasmvaultsyncclient_unsubscribe: (a: number, b: number) => [number, number];
    readonly wasmvaultsyncclient_update: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly wasmvaultsyncclient_workingSets: (a: number) => number;
    readonly wasmvaultsyncclient_workspace: (a: number) => number;
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
    readonly wasm_bindgen__convert__closures_____invoke__h7bc44194b3ab93f4: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h38ebcc3efbfba5e6: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h7bc44194b3ab93f4_3: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h2178f200c4e67708: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h2178f200c4e67708_5: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h2178f200c4e67708_6: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h51f55c9a10f889b7: (a: number, b: number) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h22468ced884afd53: (a: number, b: number) => void;
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
