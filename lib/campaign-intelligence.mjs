// Craft-specific evidence interpretation; no provider calls or sending capability.
export const CATEGORIES = ['coffee', 'wine', 'cocktail', 'whiskey', 'beer'];
export const ANGLES = {
  seasonal: {label: 'A timely reason to treat yourself', action: 'Use a relevant seasonal occasion to make the festival feel like a personal treat. The historical offer also included early-bird pricing; the subject alone did not produce the result.', subject: 'Your next great [category] discovery awaits', outline: 'Open with the occasion, describe the experience, then give one clear ticket link. Include an offer only if it is currently valid.', condition: 'Seasonal evidence: adapt the occasion; do not repeat a Valentine message out of season.'},
  local: {label: 'Give the city a reason to celebrate', action: 'Tie the invitation to a real local moment and make attending with friends the next step.', subject: '[City], let’s make a day of it', outline: 'Name the genuine local moment, connect it to this festival, and invite the reader to bring their crew.', condition: 'Only use a local milestone that actually happened. The Eagles celebration cannot be generalized to every city or date.'},
  venue: {label: 'Lead with the distinctive experience', action: 'Make the setting or experience the hook, then explain what the ticket includes.', subject: '[Category] tasting at [verified venue]? Yes.', outline: 'Name the venue and the actual tasting experience. Finish with the correct date and ticket link.', condition: 'Venue-specific evidence; verify that the upcoming edition offers the same experience.'},
  lineup: {label: 'Make discovery the reason to come', action: 'Show what people can discover at this festival. Lead with the lineup or the chance to find a new favorite.', subject: 'Who will be pouring in [City]?', outline: 'Name a few confirmed participants, explain what guests can taste, and invite them to explore together. Give one clear ticket link.', condition: 'Verify the current lineup. Historical superlatives and sold-out claims are not permission to repeat them.'},
  value: {label: 'Make the ticket’s value concrete', action: 'Lead with what is included, then make it easy to turn that into a plan with friends.', subject: 'Your [City] [category] plans, sorted', outline: 'List the verified inclusions, name the festival and date, then invite the reader to reserve a place.', condition: 'Past offers and included credits may differ by edition. Use only today’s verified inclusions.'},
  deadline: {label: 'Give a genuine reason to book now', action: 'Pair a useful festival detail with a real, dated pricing deadline.', subject: '[Festival]: [verified price] ends [date]', outline: 'Explain the actual price change and the experience the ticket buys. Finish with one booking link.', condition: 'Only publish a deadline confirmed in the current ticket setup; no invented urgency.'},
  soon: {label: 'Turn the approaching date into a plan', action: 'Use a concise reminder with the date, the experience, and a direct invitation to bring friends.', subject: '[Festival] is [verified day]. Who’s coming with you?', outline: 'State the correct day and location, include one compelling detail, then link to tickets.', condition: 'Check the actual event date before saying this Saturday or next week.'},
  social: {label: 'Help them organize the group', action: 'Make the reader the person who gets their friends together. Keep the invitation short and specific.', subject: 'Your crew. Your [category] plans. Sorted.', outline: 'Invite them to rally their friends, give a concrete reason to attend, and finish with the exact festival’s ticket link.', condition: 'Group invitation evidence does not establish that CRM super-spreaders respond better than other segments.'},
  general: {label: 'Test a clear, concrete invitation', action: 'Start with one verified reason to attend and a simple invitation. Treat this as a test while stronger patterns emerge.', subject: 'Make a day of [Festival]', outline: 'Name the festival, one verified highlight, the date and a single ticket link.', condition: 'No specific creative pattern is established by this evidence.'},
};

export function classifyAngle(subject = '', body = '') {
  const text = `${subject}\n${body}`.toLowerCase();
  if (/valentine|love yourself|perfect match|never gonna give you up/.test(text)) return 'seasonal';
  if (/birds win|eagles|super bowl/.test(text)) return 'local';
  if (/\bzoo\b/.test(text)) return 'venue';
  if (/lineup|line.up|who has the best|who will be pouring|best coffee|best cocktails|best bars|roasters/.test(text)) return 'lineup';
  if (/unlimited tasting|gaming credits|over 100 wines|assignment/.test(text)) return 'value';
  if (/early bird|earlybird|pricing ends|price increase|2-for-1|two.for.one/.test(text)) return 'deadline';
  if (/this saturday|next saturday|this weekend|next weekend|weeks? (away|until)|only.*week/.test(text)) return 'soon';
  if (/friends|crew|this is your sign|shaking it up|gather/.test(text)) return 'social';
  return 'general';
}

const number = x => typeof x === 'number' && Number.isFinite(x);
const pct = (n, d) => number(n) && number(d) && d > 0 && n >= 0 && n <= d ? 100 * n / d : null;
const median = values => { const a = values.filter(number).sort((a,b)=>a-b); return a.length ? (a[Math.floor((a.length-1)/2)] + a[Math.floor(a.length/2)]) / 2 : null; };
const key = r => `${r.provider}:${r.id}`;

// Null is unobserved; an observed zero, false, empty array or corrected category replaces the prior value.
export function mergeEvidence(previous, incoming) {
  const rows = new Map(previous.map(r => [key(r), {...r}]));
  for (const r of incoming) {
    if (!['mailchimp', 'eventbrite'].includes(r.provider) || !/^[a-zA-Z0-9]+$/.test(r.id || '') || !/^\d{4}-\d{2}-\d{2}/.test(r.observed_at || '')) throw new Error('Evidence identity or observation date missing');
    const old = rows.get(key(r));
    if (old && r.observed_at < old.observed_at) continue;
    rows.set(key(r), {...old, ...Object.fromEntries(Object.entries(r).filter(([,v]) => v !== null && v !== undefined))});
  }
  return [...rows.values()].sort((a,b)=>key(a).localeCompare(key(b)));
}

export function exclusionReason(r, asOf) {
  if (r.provider !== 'mailchimp') return 'separate_provider';
  if (!CATEGORIES.includes(r.category)) return 'category_unresolved';
  if (r.cross_category_source === true) return 'cross_category_audience';
  if (!r.content_verified || !r.subject) return 'creative_unverified';
  const sent = Date.parse(r.sent_at), end = Date.parse(`${asOf.slice(0,10)}T23:59:59Z`);
  if (!Number.isFinite(sent)) return 'send_date_unknown';
  if (sent > end || end - sent < 72 * 3600000) return 'not_mature';
  if (end - sent > 3 * 365.25 * 86400000) return 'older_than_three_years';
  if (!number(r.delivered) || r.delivered < 500) return 'under_500_deliveries';
  if (pct(r.clickers, r.delivered) === null) return 'invalid_click_metrics';
  return null;
}

function example(r) {
  return {id:r.id, provider:r.provider, subject:r.subject, audience:r.audience || 'Audience not recovered', category_basis:r.category_basis,
    sent_at:r.sent_at || null, send_label:r.send_label || null, days_out:number(r.days_out) && r.days_out >= 0 ? r.days_out : null,
    delivered:r.delivered, click_rate:r.provider === 'mailchimp' ? pct(r.clickers,r.delivered) : r.click_rate,
    unsubscribe_rate:r.provider === 'mailchimp' ? pct(r.unsubscribes,r.delivered) : r.unsubscribe_rate,
    bounce_rate:r.bounce_rate ?? null, content_verified:r.content_verified === true, angle:r.angle ?? null,
    reported_revenue:r.provider === 'eventbrite' ? r.reported_revenue ?? null : null,
    reported_tickets:r.provider === 'eventbrite' ? r.reported_tickets ?? null : null,
    source_url:r.source_url || null, note:r.note || null};
}

export function buildIntelligence(input, asOf) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(asOf) || !Number.isFinite(Date.parse(asOf))) throw new Error('Explicit analysis date required');
  const records = mergeEvidence([], input);
  const categories = CATEGORIES.map(category => {
    const all = records.filter(r=>r.category === category);
    const mc = all.filter(r=>r.provider === 'mailchimp');
    const eligible = mc.filter(r=>!exclusionReason(r,asOf));
    const exclusions = {};
    for (const r of mc) { const reason = exclusionReason(r,asOf); if (reason) exclusions[reason] = (exclusions[reason] || 0) + 1; }
    const baseline = median(eligible.map(r=>pct(r.clickers,r.delivered)));
    const groups = Object.keys(ANGLES).map(angle => {
      const rows = eligible.filter(r=>r.angle === angle);
      if (!rows.length) return null;
      const rates = rows.map(r=>pct(r.clickers,r.delivered));
      const subjects = new Set(rows.map(r=>r.subject.trim().toLowerCase()));
      const dates = new Set(rows.map(r=>r.sent_at.slice(0,10)));
      const unsub = median(rows.map(r=>pct(r.unsubscribes,r.delivered)));
      // Regularize toward category baseline to avoid promoting a single small outlier.
      // This is a prioritization heuristic, not statistical confidence or causal lift.
      const score = (median(rates) * rows.length + baseline * 3) / (rows.length + 3);
      const sorted = [...rows].sort((a,b)=>pct(b.clickers,b.delivered)-pct(a.clickers,a.delivered) || key(a).localeCompare(key(b)));
      const picks = [sorted[0], sorted[Math.floor(sorted.length/2)], sorted[sorted.length-1]];
      const days = rows.map(r=>r.days_out).filter(d=>number(d)&&d>=0);
      return {angle,...ANGLES[angle],campaigns:rows.length,creative_count:subjects.size,send_days:dates.size,
        evidence: rows.length >= 3 && subjects.size >= 2 && dates.size >= 2 ? 'Repeated engagement signal' : 'Small sample — test idea',
        median_click_rate:median(rates),median_unsubscribe_rate:unsub,score,
        observed_timing:days.length ? {count:days.length,min:Math.min(...days),max:Math.max(...days)} : null,
        examples:[...new Map(picks.map(r=>[key(r),example(r)])).values()]};
    }).filter(Boolean).sort((a,b)=>b.score-a.score || b.campaigns-a.campaigns || a.angle.localeCompare(b.angle));
    const eb = all.filter(r=>r.provider === 'eventbrite').map(example);
    return {category,total:all.length,mailchimp:mc.length,eventbrite:eb.length,eligible:eligible.length,exclusions,
      baseline_click_rate:baseline,groups,
      eventbrite_examples:eb.sort((a,b)=>(b.reported_tickets ?? -1)-(a.reported_tickets ?? -1)),
      audience_note:'Learn creative across this category; build recipients separately for the exact city, festival type and edition. Cross-category audience sends are excluded from the Mailchimp ranking. Unknown or broad audience scopes remain disclosed in each example.'};
  });
  return {version:1,as_of:asOf,records:records.length,mailchimp:records.filter(r=>r.provider==='mailchimp').length,
    eventbrite:records.filter(r=>r.provider==='eventbrite').length,
    last_observed:records.map(r=>r.observed_at).sort().at(-1) || null,categories};
}
