// Import aggregate observations only. Raw content is used for classification, never persisted here.
import { readFileSync, writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { classifyAngle, mergeEvidence } from '../lib/campaign-intelligence.mjs';

const clean = s => typeof s === 'string' ? s.replace(/[\w.+-]+@[\w.-]+\.[a-z]{2,}/gi, '[email removed]').slice(0,500) : null;
const finite = n => typeof n === 'number' && Number.isFinite(n) && n >= 0 ? n : null;
const percent = n => finite(n) !== null && n <= 100 ? n : null;
export function normalizeMailchimp(r, observed_at) {
  return {provider:'mailchimp',id:r.campaign_id,observed_at,category:r.category,category_basis:r.category_evidence,
    subject:clean(r.subject),audience:clean(r.audience_name),sent_at:r.send_time || null,
    days_out:Number.isInteger(r.days_before_event_local_date) ? r.days_before_event_local_date : null,
    delivered:finite(r.delivered_estimate),clickers:finite(r.unique_clickers),unsubscribes:finite(r.unsubscribed),
    cross_category_source:typeof r.cross_category_source_flag === 'boolean' ? r.cross_category_source_flag : null,
    content_verified:typeof r.report_content_collected === 'boolean' ? r.report_content_collected : null,
    angle:r.body_text != null ? classifyAngle(r.subject, r.body_text) : null,
    // Provider ecommerce zeros are intentionally not converted into ticket-sale observations.
    source_url: safeArchive(r.archive_url)};
}
function safeArchive(s) {
  try { const u = new URL(s); return ['http:','https:'].includes(u.protocol) && (u.hostname === 'eepurl.com' || u.hostname.endsWith('.campaign-archive.com')) ? u.href : null; } catch { return null; }
}
export function normalizeEventbrite(r, observed_at) {
  const brand = `${r.event_name || ''} ${r.sender || ''}`.toLowerCase();
  const cats = ['coffee','wine','cocktail','whiskey','beer'].filter(c=>new RegExp(`\\b${c}\\b`).test(brand));
  return {provider:'eventbrite',id:r.id,observed_at,category:cats.length === 1 ? cats[0] : 'needs_review',
    category_basis:r.event_name ? 'preview_event' : 'sender_brand_provisional',
    subject:clean(r.subject),audience:Array.isArray(r.audience_lists) ? clean(r.audience_lists.join(', ')) : null,
    sent_at:r.verified_sent_at || null,send_label:clean(r.send_label),days_out:Number.isInteger(r.verified_days_out) && r.verified_sent_at ? r.verified_days_out : null,
    delivered:finite(r.delivered),click_rate:percent(r.click_rate),unsubscribe_rate:percent(r.unsubscribe_rate),bounce_rate:percent(r.bounce_rate),
    reported_revenue:finite(r.revenue),reported_tickets:finite(r.ticket_sales),content_verified:typeof r.content_verified === 'boolean' ? r.content_verified : null,
    angle:r.content_verified === true ? classifyAngle(r.subject,r.body) : null,
    source_url:/^\d+$/.test(r.id) ? `https://www.eventbrite.com/organizations/campaigns/email/${r.id}` : null,note:clean(r.note)};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [provider, inputPath, observed, outputPath = 'data/campaign-evidence.json'] = process.argv.slice(2);
  if (!['mailchimp','eventbrite'].includes(provider) || !inputPath || !/^\d{4}-\d{2}-\d{2}(T.*)?$/.test(observed || '') || !Number.isFinite(Date.parse(observed))) throw new Error('Usage: node scripts/import-campaign-evidence.mjs mailchimp|eventbrite input.json observed-date [output.json]');
  const input = JSON.parse(readFileSync(inputPath,'utf8'));
  const rows = input.campaigns || input.records || input;
  if (!Array.isArray(rows)) throw new Error('Expected campaigns or records array');
  let previous = {records:[]};
  try { previous = JSON.parse(readFileSync(outputPath,'utf8')); } catch (e) { if (e.code !== 'ENOENT') throw e; }
  const normalized = rows.map(r=>(provider === 'mailchimp' ? normalizeMailchimp : normalizeEventbrite)(r,observed));
  const records = mergeEvidence(previous.records,normalized);
  writeFileSync(outputPath,JSON.stringify({version:1,records},null,2)+'\n');
  console.log(`Imported ${normalized.length} aggregate observations; ${records.length} unique campaigns retained.`);
}
