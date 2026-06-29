/* tslint:disable */
/* eslint-disable */

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
    define_schema(doc_id: string, schema_json: string): Promise<void>;
    delete(doc_id: string, record_id: string): Promise<void>;
    find(doc_id: string): Promise<Array<any>>;
    fire_subscription(doc_id: string, record_id: string): Promise<void>;
    get(doc_id: string, record_id: string): Promise<any>;
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
    rotate_keys(): Promise<any>;
    shutdown(): Promise<void>;
    subscribe(doc_id: string, callback: Function): WasmSubscriptionHandle;
    sync_status(): Promise<any>;
    unsubscribe(handle: WasmSubscriptionHandle): void;
    update(doc_id: string, record_id: string, json: string): Promise<void>;
}

export function decrypt(ciphertext: Uint8Array, recipient_sk: Uint8Array, sender_pk: Uint8Array): Uint8Array;

export function encrypt(plaintext: Uint8Array, sender_sk: Uint8Array, recipient_pk: Uint8Array): Uint8Array;

export function init(): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_wasmsubscriptionhandle_free: (a: number, b: number) => void;
    readonly __wbg_wasmvaultsyncclient_free: (a: number, b: number) => void;
    readonly decrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly encrypt: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly init: () => void;
    readonly wasmsubscriptionhandle_cancel: (a: number, b: number) => [number, number];
    readonly wasmvaultsyncclient_active_key_version: (a: number) => bigint;
    readonly wasmvaultsyncclient_define_schema: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_delete: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_find: (a: number, b: number, c: number) => any;
    readonly wasmvaultsyncclient_fire_subscription: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_get: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmvaultsyncclient_insert: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly wasmvaultsyncclient_is_leader: (a: number) => number;
    readonly wasmvaultsyncclient_list_key_versions: (a: number) => [number, number, number];
    readonly wasmvaultsyncclient_metricsSnapshot: (a: number) => [number, number];
    readonly wasmvaultsyncclient_new: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => any;
    readonly wasmvaultsyncclient_new_with_coordinator: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number, k: number, l: number) => any;
    readonly wasmvaultsyncclient_presence: (a: number) => number;
    readonly wasmvaultsyncclient_prune_key_versions: (a: number, b: number) => [number, number];
    readonly wasmvaultsyncclient_rotate_keys: (a: number) => any;
    readonly wasmvaultsyncclient_shutdown: (a: number) => any;
    readonly wasmvaultsyncclient_subscribe: (a: number, b: number, c: number, d: any) => number;
    readonly wasmvaultsyncclient_sync_status: (a: number) => any;
    readonly wasmvaultsyncclient_unsubscribe: (a: number, b: number) => [number, number];
    readonly wasmvaultsyncclient_update: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly __wbg_wasmipc_free: (a: number, b: number) => void;
    readonly wasmipc_new: (a: number, b: number) => [number, number, number];
    readonly wasmipc_on_message: (a: number, b: any) => void;
    readonly wasmipc_receive: (a: number) => [number, number];
    readonly wasmipc_send: (a: number, b: number, c: number) => [number, number];
    readonly __wbg_presencemanager_free: (a: number, b: number) => void;
    readonly presencemanager_activePeers: (a: number) => [number, number];
    readonly presencemanager_new: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly presencemanager_peer_count: (a: number) => number;
    readonly wasm_bindgen__convert__closures_____invoke__h685410aed2fde3f1: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h6742839cb717cdad: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h2e344701028fdaf2: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__hde300ef533c5520f: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h2e344701028fdaf2_3: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h9e7fb665684ebcac: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h9e7fb665684ebcac_5: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h9e7fb665684ebcac_6: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h719b5b51b5754f1f: (a: number, b: number) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h7c8b54782b404fc3: (a: number, b: number) => void;
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
