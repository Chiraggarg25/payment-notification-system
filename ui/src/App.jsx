import { useCallback, useEffect, useState } from 'react';
import { api } from './api.js';

const PAYMENT_STATUSES = ['SUCCESS', 'FAILED', 'PENDING', 'REFUNDED'];
const VENDOR_MODES = ['UP', 'DOWN', 'FLAKY', 'TIMEOUT', 'REJECT'];
const MODE_HELP = {
  UP: '200 OK',
  DOWN: '503 - retried, trips circuit breaker',
  FLAKY: '50% of calls return 500',
  TIMEOUT: 'sleeps 5s (client times out at 2s)',
  REJECT: '400 - not retried, goes to DEAD',
};
const FALLBACK_PLATFORMS = ['udemy', 'amazon', 'coursera'];

const newTxnId = () => `TXN-${Date.now().toString(36).toUpperCase()}-${Math.random().toString(36).slice(2, 6).toUpperCase()}`;
const newKey = () => (window.crypto && window.crypto.randomUUID
  ? window.crypto.randomUUID()
  : `${Date.now()}-${Math.random().toString(36).slice(2)}`);
const time = (iso) => (iso ? new Date(iso).toLocaleTimeString() : '—');

function usePolling(fn, intervalMs, deps = []) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const load = useCallback(async () => {
    try {
      setData(await fn());
      setError(null);
    } catch (e) {
      setError(e.message);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  useEffect(() => {
    load();
    const id = setInterval(load, intervalMs);
    return () => clearInterval(id);
  }, [load, intervalMs]);
  return { data, error, reload: load };
}

function Badge({ value }) {
  return <span className={`badge badge-${String(value).toLowerCase()}`}>{value}</span>;
}

function Card({ title, subtitle, actions, children }) {
  return (
    <section className="card">
      <header className="card-head">
        <div>
          <h2>{title}</h2>
          {subtitle && <p className="muted">{subtitle}</p>}
        </div>
        {actions && <div className="actions">{actions}</div>}
      </header>
      {children}
    </section>
  );
}

function SendPanel({ platforms, onSent }) {
  const [form, setForm] = useState({ platformId: 'udemy', transactionId: newTxnId(), paymentStatus: 'SUCCESS' });
  const [busy, setBusy] = useState(false);
  const [log, setLog] = useState([]);

  const push = (entry) => setLog((l) => [entry, ...l].slice(0, 15));

  const send = async (payload) => {
    try {
      const r = await api.publish(payload, newKey());
      push({ ok: true, ...r });
    } catch (e) {
      push({ ok: false, ...payload, error: e.message });
    }
  };

  const submit = async (e) => {
    e.preventDefault();
    setBusy(true);
    await send(form);
    setForm((f) => ({ ...f, transactionId: newTxnId() }));
    setBusy(false);
    onSent();
  };

  const burst = async () => {
    setBusy(true);
    for (let i = 0; i < 5; i++) {
      await send({ ...form, transactionId: newTxnId() });
    }
    setBusy(false);
    onSent();
  };

  return (
    <Card title="1 · Send payment status" subtitle="POST → platform-service → Kafka topic payment-notifications.{platformId} (key = platformId)">
      <form className="send-form" onSubmit={submit}>
        <label>
          Platform ID
          <select value={form.platformId} onChange={(e) => setForm({ ...form, platformId: e.target.value })}>
            {platforms.map((p) => <option key={p} value={p}>{p}</option>)}
          </select>
        </label>
        <label className="grow">
          Transaction ID
          <div className="row">
            <input value={form.transactionId} required
                   onChange={(e) => setForm({ ...form, transactionId: e.target.value })} />
            <button type="button" className="ghost" onClick={() => setForm({ ...form, transactionId: newTxnId() })}>
              New
            </button>
          </div>
        </label>
        <label>
          Payment status
          <select value={form.paymentStatus} onChange={(e) => setForm({ ...form, paymentStatus: e.target.value })}>
            {PAYMENT_STATUSES.map((s) => <option key={s}>{s}</option>)}
          </select>
        </label>
        <div className="row buttons">
          <button type="submit" disabled={busy}>Send</button>
          <button type="button" className="secondary" disabled={busy} onClick={burst}>Send 5 (new txns)</button>
        </div>
      </form>

      {log.length > 0 && (
        <table className="compact">
          <thead>
            <tr><th>Platform</th><th>Txn</th><th>Status</th><th>Kafka topic-partition @ offset</th><th>Event id</th></tr>
          </thead>
          <tbody>
            {log.map((r, i) => (
              <tr key={i} className={r.ok ? '' : 'err'}>
                <td>{r.platformId}</td>
                <td className="mono">{r.transactionId}</td>
                <td>{r.paymentStatus}</td>
                <td className="mono">{r.ok ? `${r.topic}-${r.partition} @ ${r.offset}` : r.error}</td>
                <td className="mono small">{r.ok && r.eventId ? r.eventId.slice(0, 8) : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

function VendorPanel() {
  const { data, error, reload } = usePolling(api.vendors, 2000);
  const [actionError, setActionError] = useState(null);
  const setMode = async (p, m) => {
    try { await api.setVendorMode(p, m); setActionError(null); } catch (e) { setActionError(e.message); }
    reload();
  };
  return (
    <Card title="2 · Platform APIs (mock vendors)" subtitle="Break a platform to see retries, circuit breaker and Redis"
          actions={<button className="ghost" onClick={async () => {
            try { await api.resetVendors(); } catch (e) { setActionError(e.message); }
            reload();
          }}>Reset</button>}>
      {(error || actionError) && <p className="error">mock-vendor-api: {error || actionError}</p>}
      <div className="vendors">
        {(data || []).map((v) => (
          <div key={v.name} className="vendor">
            <div className="vendor-head">
              <strong>{v.name}</strong>
              <Badge value={v.mode} />
            </div>
            <div className="segmented">
              {VENDOR_MODES.map((m) => (
                <button key={m} title={MODE_HELP[m]} className={v.mode === m ? 'active' : ''}
                        onClick={() => setMode(v.name, m)}>{m}</button>
              ))}
            </div>
            <div className="muted small">
              calls {v.totalCalls} · acked {v.acknowledged} · failed {v.failed}
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}

function CircuitBreakerPanel() {
  const { data, error, reload } = usePolling(api.circuitBreakers, 1500);
  const consumers = usePolling(api.consumers, 5000);
  const consumerOf = (name) => (consumers.data || []).find((c) => c.platformId === name);
  return (
    <Card title="3 · Consumers & circuit breakers"
          subtitle="notification-service · one Kafka topic, consumer group and circuit breaker per platform">
      {error && <p className="error">notification-service: {error}</p>}
      <div className="table-wrap">
        <table className="compact">
          <thead>
            <tr>
              <th>Platform</th><th>Kafka topic</th><th>Consumer group</th><th>Assigned</th>
              <th>Breaker</th><th>Failure rate</th><th>Buffered</th><th>Failed</th><th>Short-circuited</th><th />
            </tr>
          </thead>
          <tbody>
            {(data || []).map((cb) => {
              const c = consumerOf(cb.name);
              return (
                <tr key={cb.name}>
                  <td>{cb.name}</td>
                  <td className="mono">{c ? c.topic : '—'}</td>
                  <td className="mono">{c ? c.groupId : '—'}</td>
                  <td className="mono small">{c ? ((c.assignedPartitions || []).join(', ') || (c.running ? 'rebalancing…' : 'stopped')) : '—'}</td>
                  <td><Badge value={cb.state} /></td>
                  <td>{cb.failureRate < 0 ? '—' : `${cb.failureRate.toFixed(0)}%`}</td>
                  <td>{cb.bufferedCalls}</td>
                  <td>{cb.failedCalls}</td>
                  <td>{cb.notPermittedCalls}</td>
                  <td><button className="ghost small" onClick={async () => {
                    try { await api.resetCircuitBreaker(cb.name); } catch { /* shown by the next poll */ }
                    reload();
                  }}>Reset</button></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function NotificationsPanel({ refreshKey }) {
  const [filter, setFilter] = useState('');
  const list = usePolling(() => api.notifications(filter), 2000, [filter, refreshKey]);
  const stats = usePolling(api.stats, 2000, [refreshKey]);
  const [sweepMsg, setSweepMsg] = useState(null);

  const sweep = async () => {
    let r;
    try {
      r = await api.sweepNow();
    } catch (e) {
      setSweepMsg(`Sweep failed: ${e.message}`);
      return;
    }
    setSweepMsg(`Sweep: claimed ${r.claimed}, delivered ${r.delivered}, rescheduled ${r.rescheduled}, ` +
      `postponed ${r.postponed}, dead ${r.dead}, busy ${r.busy}, errors ${r.errors}, timed out ${r.timedOut}`);
    list.reload(); stats.reload();
  };
  const clear = async () => {
    if (!window.confirm('Delete all notification keys from Redis?')) return;
    try { await api.clearRedis(); } catch (e) { setSweepMsg(`Clear failed: ${e.message}`); }
    list.reload(); stats.reload();
  };

  const s = stats.data;
  const count = (k) => (s && s.byStatus && s.byStatus[k] != null ? s.byStatus[k] : 0);
  return (
    <Card title="4 · Redis: notification state"
          subtitle="Written by notification-service, retried by sweeper-service (exponential backoff → DEAD)"
          actions={<>
            <button className="secondary" onClick={sweep}>Sweep now</button>
            <button className="ghost" onClick={clear}>Clear Redis</button>
          </>}>
      {(list.error || stats.error) && <p className="error">sweeper-service: {list.error || stats.error}</p>}
      {s && (
        <div className="stats">
          <div className="stat"><span>{count('DELIVERED')}</span>DELIVERED</div>
          <div className="stat warn"><span>{count('RETRY_PENDING')}</span>RETRY_PENDING</div>
          <div className="stat bad"><span>{count('DEAD')}</span>DEAD</div>
          <div className="stat"><span>{s.retryQueueSize}</span>in retry-queue (ZSET)</div>
          <div className="stat"><span>{s.deadSetSize}</span>in dead (SET)</div>
        </div>
      )}
      {sweepMsg && <p className="muted small">{sweepMsg}</p>}
      <div className="tabs">
        {['', 'DELIVERED', 'RETRY_PENDING', 'DEAD'].map((f) => (
          <button key={f || 'ALL'} className={filter === f ? 'active' : ''} onClick={() => setFilter(f)}>{f || 'ALL'}</button>
        ))}
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Platform</th><th>Txn</th><th>Payment</th><th>Delivery</th>
              <th title="attempts made by the Kafka consumer (retry)">Consumer tries</th>
              <th title="attempts made by the sweeper job">Sweeps</th>
              <th>Next retry</th><th>Updated</th><th>Last error</th><th />
            </tr>
          </thead>
          <tbody>
            {(list.data || []).length === 0 && (
              <tr><td colSpan={10} className="muted center">No notifications in Redis yet</td></tr>
            )}
            {(list.data || []).map((n) => (
              <tr key={`${n.platformId}:${n.transactionId}`}>
                <td>{n.platformId}</td>
                <td className="mono">{n.transactionId}</td>
                <td>{n.paymentStatus}</td>
                <td><Badge value={n.deliveryStatus} />{n.deliveredBy && <span className="muted small"> by {n.deliveredBy}</span>}</td>
                <td className="center">{n.consumerAttempts}</td>
                <td className="center">{n.sweepAttempts}</td>
                <td>{n.deliveryStatus === 'RETRY_PENDING' ? time(n.nextRetryAt) : '—'}</td>
                <td>{time(n.updatedAt)}</td>
                <td className="error-cell" title={n.lastError || ''}>{n.lastError || ''}</td>
                <td>
                  {n.deliveryStatus === 'DEAD' && (
                    <button className="ghost small" onClick={async () => {
                      try { await api.requeue(n.platformId, n.transactionId); } catch (e) { setSweepMsg(`Requeue: ${e.message}`); }
                      list.reload();
                    }}>
                      Requeue
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

export default function App() {
  const [platforms, setPlatforms] = useState(FALLBACK_PLATFORMS);
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    api.platforms().then((p) => p && p.length && setPlatforms(p)).catch(() => {});
  }, []);

  return (
    <div className="page">
      <header className="page-head">
        <h1>Payment Notification Console</h1>
        <p className="muted">
          UI → platform-service → Kafka (topic per platform) → notification-service (consumer per platform, retry + circuit breaker) → platform API ·
          failures → Redis → sweeper-service
        </p>
      </header>
      <div className="grid">
        <SendPanel platforms={platforms} onSent={() => setRefreshKey((k) => k + 1)} />
        <VendorPanel />
      </div>
      <CircuitBreakerPanel />
      <NotificationsPanel refreshKey={refreshKey} />
    </div>
  );
}
