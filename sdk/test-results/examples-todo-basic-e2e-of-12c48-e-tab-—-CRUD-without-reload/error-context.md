# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: examples\todo-basic\e2e\offline.spec.ts >> Offline support >> Single tab — CRUD without reload
- Location: examples\todo-basic\e2e\offline.spec.ts:4:3

# Error details

```
Error: page.goto: Protocol error (Page.navigate): Cannot navigate to invalid URL
Call log:
  - navigating to "/", waiting until "load"

```

# Test source

```ts
  1  | import { test, expect } from "@playwright/test"
  2  | 
  3  | test.describe("Offline support", () => {
  4  |   test("Single tab — CRUD without reload", async ({ page }) => {
  5  |     page.on('console', msg => console.log('BROWSER LOG:', msg.text()));
  6  |     page.on('pageerror', err => console.error('BROWSER ERROR:', err.message));
> 7  |     await page.goto("/")
     |                ^ Error: page.goto: Protocol error (Page.navigate): Cannot navigate to invalid URL
  8  |     await page.waitForFunction(() => typeof window.__DRIFT__ !== 'undefined')
  9  |     await page.evaluate(() => window.__DRIFT__.insert("crud-1", "test item"))
  10 |     await expect(page.locator("[data-id='crud-1']")).toBeVisible()
  11 |     await page.evaluate(() => window.__DRIFT__.update("crud-1", { title: "updated" }))
  12 |     await expect(page.locator("[data-id='crud-1']")).toContainText("updated")
  13 |     await page.evaluate(() => window.__DRIFT__.delete("crud-1"))
  14 |     await expect(page.locator("[data-id='crud-1']")).not.toBeVisible()
  15 |   })
  16 | 
  17 |   test("Write offline, reconnect — 10 mutations synced", async ({ page }) => {
  18 |     await page.goto("/")
  19 |     await page.waitForFunction(() => typeof window.__DRIFT__ !== 'undefined')
  20 |     await page.route("**/*push*", route => route.abort())
  21 |     for (let i = 0; i < 10; i++) {
  22 |       await page.evaluate((idx) =>
  23 |         window.__DRIFT__.insert(`q-${idx}`, `queued ${idx}`), i)
  24 |     }
  25 |     await page.unroute("**/*push*")
  26 |     await page.waitForTimeout(3000)
  27 |     for (let i = 0; i < 10; i++) {
  28 |       await expect(page.locator(`[data-id='q-${i}']`)).toBeVisible()
  29 |     }
  30 |   })
  31 | })
  32 | 
```