import { readdir, readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
const files = (await readdir('dist/assets')).map(file => `/assets/${file}`);
const shell = ['/', '/index.html', '/icon.svg', '/manifest.webmanifest', ...files];
const version = createHash('sha256').update(await readFile('dist/index.html')).digest('hex').slice(0, 16);
await writeFile('dist/sw.js', `const CACHE='inbox-shell-${version}';
const SHELL=${JSON.stringify(shell)};
self.addEventListener('install',event=>{event.waitUntil(caches.open(CACHE).then(cache=>cache.addAll(SHELL)).then(()=>self.skipWaiting()));});
self.addEventListener('activate',event=>{event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(key=>key.startsWith('inbox-shell-')&&key!==CACHE).map(key=>caches.delete(key)))).then(()=>self.clients.claim()));});
self.addEventListener('fetch',event=>{
 const url=new URL(event.request.url);
 if(event.request.method!=='GET'||url.origin!==self.location.origin||url.search)return;
 if(event.request.mode==='navigate'&&(url.pathname==='/'||url.pathname==='/domains'||url.pathname.startsWith('/mail/'))){event.respondWith(caches.open(CACHE).then(cache=>cache.match('/index.html')));return;}
 if(!SHELL.includes(url.pathname))return;
 event.respondWith(caches.open(CACHE).then(async cache=>(await cache.match(url.pathname))||fetch(event.request)));
});`);
