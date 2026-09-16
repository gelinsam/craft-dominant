import {test} from 'node:test';
import assert from 'node:assert/strict';
import {buildIntelligence,mergeEvidence,exclusionReason,classifyAngle} from '../lib/campaign-intelligence.mjs';
import {normalizeMailchimp,normalizeEventbrite} from '../scripts/import-campaign-evidence.mjs';
const row = (extra={}) => ({provider:'mailchimp',id:'a1',observed_at:'2026-09-16',category:'coffee',subject:'Coffee lineup',angle:'lineup',content_verified:true,sent_at:'2026-08-01T10:00:00Z',delivered:1000,clickers:100,unsubscribes:3,cross_category_source:false,...extra});
const coffee = rows=>buildIntelligence(rows,'2026-09-16').categories[0];

test('upserts use provider and campaign identity, preserve unknowns, and accept observed zero corrections',()=>{
  const initial=row();
  const merged=mergeEvidence([initial],[row({clickers:0,delivered:null}),{provider:'eventbrite',id:'a1',observed_at:'2026-09-16',reported_tickets:2}]);
  assert.equal(merged.length,2);
  const mc=merged.find(r=>r.provider==='mailchimp');assert.equal(mc.clickers,0);assert.equal(mc.delivered,1000);
  assert.equal(mergeEvidence(merged,[row({observed_at:'2026-09-15',clickers:999})]).find(r=>r.provider==='mailchimp').clickers,0);
});
test('a corrected observation changes the leading idea without double-counting',()=>{
  const first=[row(),row({id:'a2',angle:'social',clickers:50})];
  assert.equal(coffee(first).groups[0].angle,'lineup');
  const next=mergeEvidence(first,[row({observed_at:'2026-09-17',clickers:1})]);
  assert.equal(coffee(next).groups[0].angle,'social');assert.equal(coffee(next).total,2);
  assert.deepEqual(coffee(mergeEvidence(next,next)),coffee(next));
});
test('small, mixed, unknown, invalid, immature and old observations do not become ranking evidence',()=>{
  for (const extra of [{delivered:499},{cross_category_source:true},{clickers:null},{clickers:1001},{clickers:-1},{sent_at:null},{sent_at:'2026-09-16'},{sent_at:'2027-01-01'},{sent_at:'2020-01-01'},{content_verified:false}]) assert.ok(exclusionReason(row(extra),'2026-09-16'));
  assert.equal(exclusionReason(row({clickers:0}),'2026-09-16'),null);
});
test('provider revenue and click rates cannot contaminate Mailchimp comparisons',()=>{
  const c=coffee([row(),row({provider:'eventbrite',id:'123',click_rate:99,reported_revenue:100000,reported_tickets:1000,sent_at:null})]);
  assert.equal(c.baseline_click_rate,10);assert.equal(c.eligible,1);assert.equal(c.eventbrite,1);
  assert.equal(c.eventbrite_examples[0].days_out,null);
});
test('category learning never merges festival types',()=>{
  const result=buildIntelligence([row(),row({id:'wine',category:'wine',clickers:1})],'2026-09-16');
  assert.equal(result.categories[0].baseline_click_rate,10);assert.equal(result.categories[1].baseline_click_rate,.1);
});
test('missing timing stays unknown and a repeated subject stays a small creative sample',()=>{
  const c=coffee([row(),row({id:'a2'}),row({id:'a3'})]);
  assert.equal(c.groups[0].observed_timing,null);assert.match(c.groups[0].evidence,/Small sample/);
});
test('negative and unknown lead times are excluded while verified local dates survive',()=>{
  const g=coffee([row({days_out:null}),row({id:'a2',days_out:-5}),row({id:'a3',days_out:17})]).groups[0];
  assert.deepEqual(g.observed_timing,{count:1,min:17,max:17});
});
test('includes a weak example, not only the most successful creative',()=>{
  const g=coffee([row(),row({id:'a2',clickers:40}),row({id:'a3',clickers:0})]).groups[0];
  assert.deepEqual(g.examples.map(e=>e.click_rate),[10,4,0]);
});
test('normalization removes arbitrary fields and recipient emails; ecommerce zero remains unattributed',()=>{
  const r=normalizeMailchimp({campaign_id:'abc',subject:'sam@example.com',body_text:'lineup',archive_url:'javascript:alert(1)',recipient_email:'private@example.com',api_key:'secret',provider_ecommerce:{total_revenue:0}},'2026-09-16');
  assert.equal(r.subject,'[email removed]');assert.equal(r.source_url,null);assert.equal(r.reported_revenue,undefined);assert.equal(r.recipient_email,undefined);assert.equal(r.api_key,undefined);
});
test('Eventbrite event year is not a verified send year and malformed rates stay unknown',()=>{
  const r=normalizeEventbrite({id:'123',sender:'DC Coffee Festival',send_label:'Aug 21',event_start:'2025-10-11',click_rate:101,revenue:0,content_verified:false},'2026-09-16');
  assert.equal(r.sent_at,null);assert.equal(r.days_out,null);assert.equal(r.click_rate,null);assert.equal(r.reported_revenue,0);assert.equal(r.angle,null);
});
test('creative grouping uses message evidence, including conditional seasonal and city hooks',()=>{
  assert.equal(classifyAngle('never gonna give you up','Love yourself'),'seasonal');
  assert.equal(classifyAngle('Birds Win!',''),'local');
  assert.equal(classifyAngle('Something special','Wine tasting at the zoo'),'venue');
});
