import { defineConfig, devices } from "@playwright/test"

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  use: {
    baseURL: "http://localhost:3000",
    trace: "on-first-retry",
    headless: true,
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
  ],
  webServer: [
    {
      command: "cargo run -p drift-coordinator-server -- --backend memory --port 9876",
      port: 9876,
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
      cwd: "../../..",
    },
    {
      command: "npx vite --port 3000",
      port: 3000,
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
      cwd: ".",
    },
  ],
})
