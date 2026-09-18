import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startAttentionPolling } from '../lib/attention.mjs';

const data = { opportunities: [], actions: [], generated_at: '2026-09-18T13:30:00Z', data_quality: { zero_is_trustworthy: true } };
const flush = () => new Promise(resolve => setImmediate(resolve));
function setup() {
  let now = 0, id = 0, visible = true;
  const timers = new Map(), calls = [], changes = [];
  const poll = startAttentionPolling({
    onChange: x => changes.push(x), isVisible: () => visible,
    schedule(fn, delay) { const key = ++id; timers.set(key, {fn, at: now + delay}); return key; },
    cancel(key) { timers.delete(key); },
    fetchImpl(url, {signal}) {
      return new Promise((resolve, reject) => {
        calls.push({url, signal, resolve, reject});
        signal.addEventListener('abort', () => reject(new Error('aborted')), {once:true});
      });
    },
  });
  return {poll, calls, changes, timers,
    hide() { visible = false; }, show() { visible = true; },
    async tick(ms) { now += ms; for (const [key,t] of [...timers]) if(t.at <= now) { timers.delete(key); t.fn(); } await flush(); },
    async complete(body=data, ok=true) { calls.at(-1).resolve({ok, json:async()=>body}); await flush(); },
  };
}

test('a 33-second analysis completes without overlapping refreshes', async () => {
  const x=setup(); await x.tick(33000);
  await x.poll.refresh(); await x.poll.refresh();
  assert.equal(x.calls.length,1); assert.equal(x.calls[0].signal.aborted,false);
  await x.complete(); assert.equal(x.changes.find(v=>v.summary)?.summary.generatedAt,data.generated_at);
  await x.tick(59000); assert.equal(x.calls.length,1);
  await x.tick(1000); assert.equal(x.calls.length,2); x.poll.stop();
});
test('timeout is bounded, reports failure, and retries only after settling', async () => {
  const x=setup(); await x.tick(55000);
  assert.equal(x.calls[0].signal.aborted,true); assert.ok(x.changes.some(v=>v.error));
  await x.tick(60000); assert.equal(x.calls.length,2); x.poll.stop();
});
test('HTTP and malformed data cannot produce an all-clear', async () => {
  for (const [body,ok] of [[data,false],[{},true]]) {
    const x=setup(); await x.complete(body,ok);
    assert.ok(x.changes.some(v=>v.error)); assert.ok(!x.changes.some(v=>v.summary)); x.poll.stop();
  }
});
test('hidden pages skip work; disposal aborts and suppresses late updates', async () => {
  const x=setup(); await x.complete(); x.hide(); await x.tick(60000);
  assert.equal(x.calls.length,1); x.show(); await x.tick(60000);
  assert.equal(x.calls.length,2); const before=x.changes.length; x.poll.stop(); await flush();
  assert.equal(x.calls[1].signal.aborted,true); assert.equal(x.changes.length,before); assert.equal(x.timers.size,0);
});
