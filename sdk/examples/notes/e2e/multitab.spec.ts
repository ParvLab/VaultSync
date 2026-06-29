import { test, expect } from '@playwright/test';

test.describe('Multi-tab sync', () => {
  test('Tab A write appears in Tab B', async ({ context }) => {
    const ns = `test-ns-${Date.now()}`;
    const url = `/?ns=${ns}`;

    const pageA = await context.newPage();
    const pageB = await context.newPage();

    pageA.on('console', msg => console.log('Page A (Test 1):', msg.text()));
    pageB.on('console', msg => console.log('Page B (Test 1):', msg.text()));

    await pageA.goto(url);
    await pageB.goto(url);

    await pageA.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');
    await pageB.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');

    // Insert note on Tab A
    await pageA.evaluate(() => {
      (window as any).__VAULTSYNC__.insert('notes', 'tab-1', {
        title: 'Note from Tab A',
        body: 'Body content from Tab A',
        updatedAt: Date.now()
      });
    });

    // Verify it appears in Tab B UI
    const noteSelector = '[data-testid="note-item"][data-id="tab-1"]';
    await expect(pageB.locator(noteSelector)).toBeVisible({ timeout: 10_000 });
    await expect(pageB.locator(noteSelector)).toContainText('Note from Tab A');

    // Update note on Tab B
    await pageB.evaluate(() => {
      (window as any).__VAULTSYNC__.update('notes', 'tab-1', {
        title: 'Updated in Tab B',
        body: 'Body content from Tab A',
        updatedAt: Date.now()
      });
    });

    // Verify update reflects back in Tab A UI
    await expect(pageA.locator(noteSelector)).toContainText('Updated in Tab B');
  });

  test('Conflict-free edit — CRDT preserves both field changes', async ({ context }) => {
    const ns = `test-ns-crdt-${Date.now()}`;
    const url = `/?ns=${ns}`;

    const pageA = await context.newPage();
    const pageB = await context.newPage();

    pageA.on('console', msg => console.log('Page A:', msg.text()));
    pageB.on('console', msg => console.log('Page B:', msg.text()));

    await pageA.goto(url);
    await pageB.goto(url);

    await pageA.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');
    await pageB.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');

    // Insert base note
    const noteId = 'crdt-note';
    await pageA.evaluate((id) => {
      (window as any).__VAULTSYNC__.insert('notes', id, {
        title: 'Original Title',
        body: 'Original Body',
        updatedAt: Date.now()
      });
    }, noteId);

    // Wait for base note to sync to B
    const noteSelector = `[data-testid="note-item"][data-id="${noteId}"]`;
    await expect(pageB.locator(noteSelector)).toBeVisible({ timeout: 10_000 });

    // Force both tabs offline to write concurrently without immediate sync
    await pageA.context().setOffline(true);

    // Edit title in Tab A
    await pageA.evaluate((id) => {
      (window as any).__VAULTSYNC__.update('notes', id, {
        title: 'Tab A Title Edit',
        updatedAt: Date.now()
      });
    }, noteId);

    // Edit body in Tab B
    await pageB.evaluate((id) => {
      (window as any).__VAULTSYNC__.update('notes', id, {
        body: 'Tab B Body Edit',
        updatedAt: Date.now()
      });
    }, noteId);

    // Go back online
    await pageA.context().setOffline(false);

    // Verify merge resolves conflicts cleanly on both: Title from A and Body from B should coexist!
    // Wait for synced state
    await pageA.waitForFunction(async () => {
      const status = await (window as any).__VAULTSYNC__.syncStatus();
      return status.connected && status.pendingMutations === 0;
    }, { timeout: 15_000 });

    await pageB.waitForFunction(async () => {
      const status = await (window as any).__VAULTSYNC__.syncStatus();
      return status.connected && status.pendingMutations === 0;
    }, { timeout: 15_000 });

    // Retrieve note values from both tabs and verify they are equal and merged
    const noteA = await pageA.evaluate((id) => (window as any).__VAULTSYNC__.get('notes', id), noteId);
    const noteB = await pageB.evaluate((id) => (window as any).__VAULTSYNC__.get('notes', id), noteId);

    expect(noteA.title).toBe('Tab A Title Edit');
    expect(noteA.body).toBe('Tab B Body Edit');
    expect(noteB.title).toBe('Tab A Title Edit');
    expect(noteB.body).toBe('Tab B Body Edit');
  });
});
