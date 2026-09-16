import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { summarizeCommand, draftPresentation, campaignUrl } from '../lib/attention.mjs';

test('incomplete intelligence never becomes an all-clear', () => {
  for (const body of [null, {}, {error: 'backend_timeout'}, {opportunities: []}]) {
    assert.throws(() => summarizeCommand(body));
  }
  const result = summarizeCommand({opportunities: [], generated_at: '2026-09-16', data_quality: {}});
  assert.equal(result.zeroTrusted, false);
});
test('preserves backend ranking and explicit trust decision', () => {
  const rows = [{event_id: 'b'}, {event_id: 'a'}];
  const result = summarizeCommand({opportunities: rows, generated_at: '2026-09-16', data_quality: {zero_is_trustworthy: true, warnings: ['Missing history']}});
  assert.deepEqual(result.opportunities, rows);
  assert.equal(result.zeroTrusted, true);
  assert.deepEqual(result.warnings, ['Missing history']);
});
test('missing, future or old evidence requires audience refresh', () => {
  const now = Date.parse('2026-09-17T12:00:00Z');
  for (const verified_at of [undefined, 'invalid', '2026-09-18', '2026-09-16']) {
    const result = draftPresentation({state: 'draft', verified_at}, now);
    assert.equal(result.stale, true);
    assert.match(result.label, /refresh needed/);
  }
  assert.match(draftPresentation({state: 'draft', verified_at: '2026-09-17T11:00:00Z'}, now).label, /review before scheduling/);
});
test('blocked and scheduled snapshots never become send-ready', () => {
  assert.equal(draftPresentation({state:'blocked'}).label, 'Audience blocked');
  assert.equal(draftPresentation({state:'scheduled'}).label, 'Scheduled when last checked');
});
test('campaign links cannot redirect to arbitrary sites', () => {
  assert.equal(campaignUrl('55889051'), 'https://www.eventbrite.com/organizations/campaigns/email/55889051');
  for (const id of ['//evil.example', '1?x=y', '', undefined]) assert.throws(() => campaignUrl(id));
});
test('draft registry preserves distinct festival editions and known blockers', () => {
  const drafts = JSON.parse(readFileSync(new URL('../data/festival-drafts.json', import.meta.url)));
  assert.equal(new Set(drafts.map(d=>d.campaign_id)).size, drafts.length);
  assert.equal(new Set(drafts.map(d=>d.event_id)).size, drafts.length);
  assert.equal(drafts.filter(d=>d.state==='draft').length, 13);
  const nyc = drafts.find(d=>d.campaign_id==='55889222');
  assert.equal(nyc.state, 'blocked'); assert.equal(nyc.active_recipients, 0);
  assert.equal(drafts.find(d=>d.campaign_id==='55888508').state, 'scheduled');
  assert.match(drafts.find(d=>d.campaign_id==='55889135').note, /805/);
  for(const d of drafts) assert.doesNotThrow(()=>campaignUrl(d.campaign_id));
});
