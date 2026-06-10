export interface PostgresCoordinatorConfig {
  url: string;
  authToken?: string;
}

export class PostgresCoordinator {
  readonly url: string;
  readonly authToken?: string;

  constructor(config: PostgresCoordinatorConfig) {
    this.url = config.url;
    this.authToken = config.authToken;
  }
}
