/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// The SPA is served at the Engine's root, so assets resolve from `/`. Build → `dist/`,
// which the Engine serves.
export default defineConfig({
  plugins: [svelte()],
  base: '/',
  build: { outDir: 'dist' },
  // Dev-only: proxy the Engine API to a running gateway so the console develops against a live
  // cluster. Point at a `kubectl port-forward svc/…-gateway 8081:8080`, or override with GDB_GATEWAY.
  server: { proxy: { '/v1': process.env.GDB_GATEWAY ?? 'http://localhost:8081' } },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.ts'],
    // The prefs suite re-imports the module per test (`vi.resetModules()`), which intermittently
    // exceeds vitest's 5s default on contended CI. Generous headroom; a genuine hang still fails.
    testTimeout: 20000,
    hookTimeout: 20000,
  },
});
