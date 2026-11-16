import {defineConfig} from '@playwright/test';
export default defineConfig({
  testDir: './e2e', testMatch: 'ux.spec.ts', workers: 1, timeout: 30000,
  use: {baseURL: 'http://127.0.0.1:4175'},
  webServer: {command: 'pnpm build && pnpm exec vite preview --host 127.0.0.1 --port 4175 --strictPort', url: 'http://127.0.0.1:4175', reuseExistingServer: false},
});
