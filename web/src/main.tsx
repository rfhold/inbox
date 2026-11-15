import { render } from 'solid-js/web';
import Auth from './Auth';
import { startTelemetry } from './telemetry';
import './style.css';
void startTelemetry();
render(() => <Auth />, document.getElementById('root')!);
if (import.meta.env.PROD && 'serviceWorker' in navigator) void navigator.serviceWorker.register('/sw.js').catch(() => {});
