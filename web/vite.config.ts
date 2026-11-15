import { defineConfig } from 'vite';
import solid from 'vite-plugin-solid';
export default defineConfig({ plugins: [solid()], server: { proxy: { '/inbox.v1.MailService': 'http://localhost:8080', '/config.json': 'http://localhost:8080' } } });
