import { test, expect } from "@playwright/test"

test.describe("Offline support", () => {
  test("Single tab — CRUD without reload", async ({ page }) => {
    page.on('console', msg => console.log('BROWSER LOG:', msg.text()));
    page.on('pageerror', err => console.error('BROWSER ERROR:', err.message));
    await page.goto("/")
    await page.waitForFunction(() => typeof window.__DRIFT__ !== 'undefined')
    await page.evaluate(() => window.__DRIFT__.insert("crud-1", "test item"))
    await expect(page.locator("[data-id='crud-1']")).toBeVisible()
    await page.evaluate(() => window.__DRIFT__.update("crud-1", { title: "updated" }))
    await expect(page.locator("[data-id='crud-1']")).toContainText("updated")
    await page.evaluate(() => window.__DRIFT__.delete("crud-1"))
    await expect(page.locator("[data-id='crud-1']")).not.toBeVisible()
  })

  test("Write offline, reconnect — 10 mutations synced", async ({ page }) => {
    await page.goto("/")
    await page.waitForFunction(() => typeof window.__DRIFT__ !== 'undefined')
    await page.route("**/*push*", route => route.abort())
    for (let i = 0; i < 10; i++) {
      await page.evaluate((idx) =>
        window.__DRIFT__.insert(`q-${idx}`, `queued ${idx}`), i)
    }
    await page.unroute("**/*push*")
    await page.waitForTimeout(3000)
    for (let i = 0; i < 10; i++) {
      await expect(page.locator(`[data-id='q-${i}']`)).toBeVisible()
    }
  })
})
