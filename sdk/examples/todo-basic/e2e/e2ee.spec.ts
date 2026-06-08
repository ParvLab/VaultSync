import { test, expect } from "@playwright/test"

test.describe("E2EE verification", () => {
  test("Coordinator sees only ciphertext — no plaintext in network traffic", async ({ page }) => {
    const bodies: string[] = []
    await page.route("**/api/mutations/**", async route => {
      const body = route.request().postData()
      if (body) bodies.push(body)
      await route.continue()
    })

    await page.goto("/")
    await page.waitForFunction(() => typeof window.__DRIFT__ !== 'undefined')
    await page.evaluate(() =>
      window.__DRIFT__.insert("secret-id", "top secret content"))
    await page.waitForTimeout(2000)

    for (const body of bodies) {
      expect(body).not.toContain("top secret content")
      expect(body).not.toContain("secret-id")
    }
  })
})
