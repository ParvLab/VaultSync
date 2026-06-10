export interface RedisCoordinatorConfig {
  url: string;
  authToken?: string;
}

export class RedisCoordinator {
  readonly url: string;
  readonly authToken?: string;

  constructor(config: RedisCoordinatorConfig) {
    this.url = config.url;
    this.authToken = config.authToken;
  }
}
