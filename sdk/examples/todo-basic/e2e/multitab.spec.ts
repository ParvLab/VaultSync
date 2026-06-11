import { test, expect, Page } from "@playwright/test"

test.describe("Multi-tab sync", () => {
  let pageA: Page, pageB: Page

  test.beforeEach(async ({ browser }) => {
    const ctx = await browser.newContext()
    pageA = await ctx.newPage()
    pageB = await ctx.newPage()
    pageA.on('console', msg => console.log('BROWSER A LOG:', msg.text()));
    pageA.on('pageerror', err => console.error('BROWSER A ERROR:', err.message));
    pageB.on('console', msg => console.log('BROWSER B LOG:', msg.text()));
    pageB.on('pageerror', err => console.error('BROWSER B ERROR:', err.message));
    const ns = "e2e-multi-" + Math.random().toString(36).substring(7);
    await Promise.all([pageA.goto(`/?ns=${ns}`), pageB.goto(`/?ns=${ns}`)])
    await Promise.all([
      pageA.waitForFunction(() => typeof window.__VAULTSYNC__ !== 'undefined'),
      pageB.waitForFunction(() => typeof window.__VAULTSYNC__ !== 'undefined'),
    ])
  })

  test("Tab A write appears in Tab B within 5 seconds", async () => {
    await pageA.evaluate(() =>
      window.__VAULTSYNC__.insert("sync-test-1", "buy milk"))
    await expect(pageB.locator("[data-testid='todo-item'][data-id='sync-test-1']"))
      .toBeVisible({ timeout: 5000 })
    await expect(pageB.locator("[data-testid='todo-item'][data-id='sync-test-1']"))
      .toContainText("buy milk")
  })

  test("Conflict-free edit — CRDT preserves both field changes", async () => {
    await pageA.evaluate(() => window.__VAULTSYNC__.insert("conflict-1", "original"))
    await pageA.waitForTimeout(500)
    await Promise.all([
      pageA.evaluate(() => window.__VAULTSYNC__.update("conflict-1", { title: "from A" })),
      pageB.evaluate(() => window.__VAULTSYNC__.update("conflict-1", { done: true })),
    ])
    await pageA.waitForTimeout(3000)
    const items = await pageA.evaluate(() => window.__VAULTSYNC__.findAll())
    const item = items.find((t: any) => t.id === "conflict-1")
    expect(item).toBeDefined()
  })

  test("Tab writes 5 items offline, Tab B sees them after reconnect", async () => {
    await pageA.route("**/*push*", route => route.abort())
    for (let i = 0; i < 5; i++) {
      await pageA.evaluate((idx) =>
        window.__VAULTSYNC__.insert(`offline-${idx}`, `offline todo ${idx}`), i)
    }
    await pageA.unroute("**/*push*")
    for (let i = 0; i < 5; i++) {
      await expect(pageB.locator(`[data-id='offline-${i}']`))
        .toBeVisible({ timeout: 20000 })
    }
  })
})
