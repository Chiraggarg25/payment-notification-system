import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// `npm run dev` (port 3000) proxies to the services running on localhost -
// the same routes nginx exposes in the Docker image.
const proxy = (target) => ({
  target,
  changeOrigin: true,
  rewrite: (path) => path.replace(/^\/api\/[^/]+/, ''),
});

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api/platform': proxy('http://localhost:8080'),
      '/api/notifier': proxy('http://localhost:8081'),
      '/api/sweeper': proxy('http://localhost:8082'),
      '/api/vendor': proxy('http://localhost:8090'),
    },
  },
});
