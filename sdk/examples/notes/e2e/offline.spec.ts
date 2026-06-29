import { test, expect } from '@playwright/test';

test.describe('Offline support', () => {
  test('Single tab — CRUD without reload', async ({ page }) => {
    await page.goto('/');
    
    // Wait for the client to initialize
    await page.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');
    
    // Insert a note
    await page.evaluate(() => {
      (window as any).__VAULTSYNC__.insert('notes', 'crud-1', {
        title: 'Test Note',
        body: 'Test Body content',
        updatedAt: Date.now()
      });
    });
    
    // Verify visibility in UI
    const noteSelector = '[data-testid="note-item"][data-id="crud-1"]';
    await expect(page.locator(noteSelector)).toBeVisible();
    await expect(page.locator(noteSelector)).toContainText('Test Note');
    
    // Update the note
    await page.evaluate(() => {
      (window as any).__VAULTSYNC__.update('notes', 'crud-1', {
        title: 'Updated Test Note',
        body: 'Updated Body content',
        updatedAt: Date.now()
      });
    });
    
    // Verify update in UI
    await expect(page.locator(noteSelector)).toContainText('Updated Test Note');
    
    // Delete the note
    await page.evaluate(() => {
      (window as any).__VAULTSYNC__.delete('notes', 'crud-1');
    });
    
    // Verify deletion in UI
    await expect(page.locator(noteSelector)).not.toBeVisible();
  });

  test('Write offline, reconnect — mutations synced', async ({ page, context }) => {
    await page.goto('/');
    await page.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');
    
    // Force context offline
    await context.setOffline(true);
    
    // Perform inserts offline
    for (let i = 0; i < 5; i++) {
      await page.evaluate(async (idx) => {
        await (window as any).__VAULTSYNC__.insert('notes', `offline-${idx}`, {
          title: `Offline Note ${idx}`,
          body: `Written while offline ${idx}`,
          updatedAt: Date.now()
        });
      }, i);
    }
    
    // Verify they are visible locally (local-first)
    for (let i = 0; i < 5; i++) {
      await expect(page.locator(`[data-testid="note-item"][data-id="offline-${i}"]`)).toBeVisible();
    }
    
    // Reconnect
    await context.setOffline(false);
    
    // Wait for connection to re-establish and status to be synced (no pending mutations)
    await page.waitForFunction(async () => {
      const status = await (window as any).__VAULTSYNC__.syncStatus();
      return status.connected && status.pendingMutations === 0;
    }, { timeout: 15_000 });
    
    // Ensure all items are still visible
    for (let i = 0; i < 5; i++) {
      await expect(page.locator(`[data-testid="note-item"][data-id="offline-${i}"]`)).toBeVisible();
    }
  });
});
