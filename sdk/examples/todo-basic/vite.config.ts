import { defineConfig } from 'vite'

export default defineConfig({
  build: {
    target: 'esnext'
  },
  server: {
    port: 3000,
    fs: {
      // Allow serving files from the workspace root (e.g. wasm files in packages/web)
      allow: ['../..']
    }
  }
})
