import type { VaultSyncRuntime } from '../wasm/vaultsync_wasm.js';

export interface KeyInfo {
  version: number;
  createdAt: number; // Unix epoch ms
  isActive: boolean;
}

export interface KeyManagerOptions {
  /** Called after a successful key rotation. */
  onRotated?: (newVersion: number, oldVersion: number) => void;
}

export class KeyManager {
  private client: any;
  private options: KeyManagerOptions;

  constructor(client: any, options: KeyManagerOptions = {}) {
    this.client = client;
    this.options = options;
  }

  /** Rotate to a new encryption key. Returns the new version. */
  async rotate(): Promise<number> {
    const newVersion: number = await this.client.rotate_keys();
    this.options.onRotated?.(newVersion, newVersion - 1);
    return newVersion;
  }

  /** List all key versions held on this replica. */
  async listVersions(): Promise<KeyInfo[]> {
    const raw: string = await this.client.list_key_versions();
    return JSON.parse(raw) as KeyInfo[];
  }

  /** Get the active key version. */
  activeVersion(): number {
    return this.client.active_key_version();
  }

  /** Prune old key versions keeping at least `keepVersions` newest keys. */
  pruneOldVersions(keepVersions: number = 2): void {
    this.client.prune_key_versions(keepVersions);
  }
}
