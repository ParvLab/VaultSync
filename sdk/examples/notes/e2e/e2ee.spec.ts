import { test, expect } from '@playwright/test';

test.describe('E2EE verification', () => {
  test('Coordinator sees only ciphertext — no plaintext in network traffic', async ({ page }) => {
    const payloads: (string | Buffer)[] = [];
    
    // Capture WebSocket frames sent from the client
    page.on('websocket', (ws) => {
      ws.on('framesent', (event) => {
        payloads.push(event.payload);
      });
    });

    const ns = `e2ee-test-${Date.now()}`;
    await page.goto(`/?ns=${ns}`);
    await page.waitForFunction(() => typeof (window as any).__VAULTSYNC__ !== 'undefined');

    const secretTitle = 'top-secret-note-title';
    const secretBody = 'This is a top secret note body content that must be encrypted!';

    // Insert note with secret content
    await page.evaluate(({ title, body }) => {
      (window as any).__VAULTSYNC__.insert('notes', 'secret-note-1', {
        title,
        body,
        updatedAt: Date.now()
      });
    }, { title: secretTitle, body: secretBody });

    // Wait for the sync engine to push mutations to the coordinator
    await page.waitForFunction(async () => {
      const status = await (window as any).__VAULTSYNC__.syncStatus();
      return status.connected && status.pendingMutations === 0;
    }, { timeout: 15_000 });

    // Assert that we captured WebSocket frames and none of them contain plaintext
    expect(payloads.length).toBeGreaterThan(0);
    for (const payload of payloads) {
      const payloadStr = typeof payload === 'string' ? payload : payload.toString('utf8');
      expect(payloadStr).not.toContain(secretTitle);
      expect(payloadStr).not.toContain(secretBody);
    }
  });
});
