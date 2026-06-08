import { test, expect } from "@playwright/test"

test.describe("Leader election", () => {
  test("Both tabs sync concurrently", async ({ browser }) => {
    const ctx = await browser.newContext()
    const tabA = await ctx.newPage()
    const tabB = await ctx.newPage()
    tabA.on('console', msg => console.log('BROWSER A LOG:', msg.text()));
    tabA.on('pageerror', err => console.error('BROWSER A ERROR:', err.message));
    tabB.on('console', msg => console.log('BROWSER B LOG:', msg.text()));
    tabB.on('pageerror', err => console.error('BROWSER B ERROR:', err.message));
    const ns = "e2e-leader-" + Math.random().toString(36).substring(7);
    await Promise.all([tabA.goto(`/?ns=${ns}`), tabB.goto(`/?ns=${ns}`)])
    await Promise.all([
      tabA.waitForFunction(() => typeof window.__DRIFT__ !== 'undefined'),
      tabB.waitForFunction(() => typeof window.__DRIFT__ !== 'undefined'),
    ])

    await tabA.evaluate(() => window.__DRIFT__.insert("pre-crash", "data"))
    await expect(tabB.locator("[data-testid='todo-item'][data-id='pre-crash']"))
      .toBeVisible({ timeout: 5000 })
  })
})
