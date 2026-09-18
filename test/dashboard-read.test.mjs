import test from 'node:test';
import assert from 'node:assert/strict';
import {readDashboard} from '../lib/dashboard-read.mjs';

const valid = {events:[], portfolio:{total_tickets:0,total_revenue:0,total_spend:0,portfolio_cac:0}, customers:{total:0}};
test('a verified empty dashboard remains a valid result', async () => {
  assert.deepEqual(await readDashboard(async () => ({ok:true,json:async()=>valid})), valid);
});
test('HTTP errors cannot become an empty dashboard', async () => {
  let parsed = false;
  await assert.rejects(readDashboard(async () => ({ok:false,json:async()=>{parsed=true;return {error:'upstream unavailable'};}})));
  assert.equal(parsed,false);
});
test('malformed successful responses are rejected', async () => {
  for (const value of [null, [], {}, {error:'unavailable'}, {...valid,portfolio:{}}, {...valid,events:null}, {...valid,customers:[]}, {...valid,events:[null]}, {...valid,portfolio:{...valid.portfolio,total_spend:'unknown'}}]) {
    await assert.rejects(readDashboard(async () => ({ok:true,json:async()=>value})));
  }
});
test('network and JSON failures remain failures', async () => {
  await assert.rejects(readDashboard(async () => {throw new Error('network');}));
  await assert.rejects(readDashboard(async () => ({ok:true,json:async()=>{throw new Error('not JSON');}})));
});
test('refresh bypasses stale HTTP cache and carries cancellation', async () => {
  const controller = new AbortController();
  await readDashboard(async (url, options) => {
    assert.equal(url,'/api/proxy/api/dashboard');
    assert.equal(options.cache,'no-store');
    assert.equal(options.signal,controller.signal);
    return {ok:true,json:async()=>valid};
  }, controller.signal);
});
