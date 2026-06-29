import { test, expect } from '@playwright/test';

test.describe('Leader election', () => {
  test('Promotion of follower when leader tab is closed', async ({ context }) => {
    const ns = `test-ns-leader-${Date.now()}`;
    const url = `/?ns=${ns}`;

    // Open first tab (becomes leader)
    const pageA = await context.newPage();
    await pageA.goto(url);
    await pageA.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');

    const roleSelector = '[data-testid="tab-role"]';
    await expect(pageA.locator(roleSelector)).toHaveAttribute('data-role', 'leader', { timeout: 15_000 });

    // Open second tab (becomes follower)
    const pageB = await context.newPage();
    await pageB.goto(url);
    await pageB.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');

    await expect(pageB.locator(roleSelector)).toHaveAttribute('data-role', 'follower', { timeout: 10_000 });

    // Close the leader tab (pageA)
    await pageA.close();

    // Verify follower (pageB) is promoted to leader
    await expect(pageB.locator(roleSelector)).toHaveAttribute('data-role', 'leader', { timeout: 15_000 });
  });
});
