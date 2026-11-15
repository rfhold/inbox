import { defineConfig } from '@playwright/test';
export default defineConfig({testDir: './e2e', testIgnore: ['offline.spec.ts', 'visual.spec.ts', 'changes.spec.ts', 'auth.spec.ts'], timeout: 60000, expect: {timeout: 15000}, use: {baseURL: process.env.BASE_URL || 'http://localhost:8080', trace: 'retain-on-failure'}, workers: 1});
