import { defineConfig } from '@playwright/test';
import config from './playwright.config';

export default defineConfig({...config, outputDir: 'test-results/integration', use: {...config.use, baseURL: 'http://localhost:4173'}, webServer: {command: 'pnpm preview --port 4173', url: 'http://localhost:4173', reuseExistingServer: false}});
