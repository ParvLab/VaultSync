import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 5173,
    fs: {
      // Allow serving files from the workspace packages directory
      allow: ['..']
    }
  }
});
