export interface StorageConfig {
  path?: string;
  inMemory?: boolean;
}

export class NodeStorage {
  readonly path: string | null;

  constructor(config: StorageConfig) {
    if (config.inMemory) {
      this.path = null;
    } else {
      this.path = config.path || './drift.db';
    }
  }
}
