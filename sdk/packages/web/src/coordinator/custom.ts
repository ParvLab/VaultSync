export interface CustomCoordinatorConfig {
  url: string;
  authToken?: string;
  headers?: Record<string, string>;
}

export class CustomCoordinator {
  readonly url: string;
  readonly authToken?: string;
  readonly headers?: Record<string, string>;

  constructor(config: CustomCoordinatorConfig) {
    this.url = config.url;
    this.authToken = config.authToken;
    this.headers = config.headers;
  }
}
