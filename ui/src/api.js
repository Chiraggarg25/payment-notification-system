// All calls go through relative /api/<service>/ prefixes:
//   nginx (docker) or the Vite dev proxy (npm run dev) forwards them to the right service.
const DEFAULT_TIMEOUT_MS = 10000;

// Every call has a timeout, so a service that is down or still starting shows an error on its card
// instead of leaving the request pending forever.
async function request(method, url, body, extraHeaders = {}, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let res;
  try {
    res = await fetch(url, {
      method,
      headers: { ...(body ? { 'Content-Type': 'application/json' } : {}), ...extraHeaders },
      body: body ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
  } catch (e) {
    const service = url.split('/')[2] || 'service';
    throw new Error(e.name === 'AbortError'
      ? `no answer from ${service} after ${timeoutMs / 1000}s (still starting?)`
      : `cannot reach ${service}`);
  } finally {
    clearTimeout(timer);
  }
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!res.ok) {
    if (res.status === 502 || res.status === 504) {
      throw new Error(`${url.split('/')[2]} is not running or not ready yet (${res.status})`);
    }
    const msg = (data && (data.detail || data.error || data.message)) || `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return data;
}

export const api = {
  // platform-service
  platforms: () => request('GET', '/api/platform/api/v1/platforms'),
  // idempotencyKey: reuse the same key when retrying the same submission -> never a duplicate notification
  publish: (payload, idempotencyKey) => request('POST', '/api/platform/api/v1/payments/notifications', payload,
    idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : {}),
  // notification-service
  circuitBreakers: () => request('GET', '/api/notifier/api/circuit-breakers'),
  consumers: () => request('GET', '/api/notifier/api/consumers'),
  resetCircuitBreaker: (name) => request('POST', `/api/notifier/api/circuit-breakers/${name}/reset`),
  // sweeper-service (Redis view)
  notifications: (status, limit = 200) =>
    request('GET', `/api/sweeper/api/notifications?limit=${limit}${status ? `&status=${status}` : ''}`),
  stats: () => request('GET', '/api/sweeper/api/notifications/stats'),
  sweepNow: () => request('POST', '/api/sweeper/api/notifications/sweep', undefined, {}, 60000),
  requeue: (p, t) => request('POST', `/api/sweeper/api/notifications/${p}/${encodeURIComponent(t)}/requeue`),
  clearRedis: () => request('DELETE', '/api/sweeper/api/notifications'),
  // mock vendor
  vendors: () => request('GET', '/api/vendor/admin/vendors'),
  setVendorMode: (p, mode) => request('PUT', `/api/vendor/admin/vendors/${p}/mode/${mode}`),
  resetVendors: () => request('POST', '/api/vendor/admin/vendors/reset'),
};
