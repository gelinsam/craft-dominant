import {readFileSync,writeFileSync} from 'node:fs';
import {buildIntelligence} from '../lib/campaign-intelligence.mjs';
const root = new URL('../',import.meta.url);
const {records} = JSON.parse(readFileSync(new URL('data/campaign-evidence.json',root),'utf8'));
// Snapshot time comes from observations, never from page refresh or redeployment.
const asOf = records.map(r=>r.observed_at).sort().at(-1)?.slice(0,10);
const report = buildIntelligence(records,asOf);
writeFileSync(new URL('data/campaign-intelligence.json',root),JSON.stringify(report,null,2)+'\n');
console.log(`Built provisional intelligence: ${report.mailchimp} Mailchimp and ${report.eventbrite} Eventbrite campaigns, as of ${asOf}.`);
