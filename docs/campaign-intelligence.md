# Provisional campaign intelligence

The Actions view now offers working creative ideas from the history already recovered. It does not wait for a complete Eventbrite export. Root pacing, campaign recipients and send behavior are unchanged. The existing Next.js/React presentation and Node test runner are reused. This is Craft-specific interpretation of existing data, not new agent, storage or orchestration infrastructure.

## Evidence

Initial import: 393 Mailchimp campaigns with reports and content, plus 11 individually inspected Eventbrite reports. The 120 Eventbrite listing rows are not imported as fully reviewed campaigns. Mailchimp raw collection had 506 campaign records, including records without reports. Categories are coffee, wine, cocktail, whiskey and beer; unresolved/other-format records remain retained but do not become category evidence.

Only aggregate allowlisted fields are saved in private deployment configuration, outside the public repository. Raw HTML, message bodies, recipient addresses, tracking parameters, API credentials and list membership are not included. Body text is used during import to assign one inspectable creative angle. Categories can be event-linked or inferred from branding; examples disclose this distinction. Audience names describe the historical scope, never authorize reuse of that audience.

Mailchimp comparisons require reviewed content, a verified send date within three years of the evidence snapshot, 72 hours of maturity, at least 500 delivered, valid unique-clicker counts and no known cross-category source audience. Unknown/broad source audiences remain eligible and are explicitly disclosed. Rates are unique clickers divided by estimated delivered. Within a category, each creative group ranks by `(median_click_rate * campaign_count + category_median * 3) / (campaign_count + 3)`. This deliberately shrinks small groups toward the baseline. It is a heuristic, not a confidence interval, causal estimate, revenue optimizer or proof of an ideal time. Medians weight sends equally; repeated audiences may overlap. A repeated signal requires three sends, two distinct subjects and two send dates. All findings remain provisional. Unsubscribe tradeoffs and strongest/median/weakest examples are displayed.

Eventbrite observations retain their provider definitions and remain separate, including small sends and unknown send years. They are ordered by reported ticket count for inspection, not ranked against Mailchimp. Provider-reported revenue is not incremental impact, and zero attribution does not establish zero sales. Mailchimp ecommerce zeros are not interpreted as observed ticket revenue. Exact days out are accepted only when verified; a year on the promoted event does not establish a send year.

## Updating

Import additional or corrected observations through the same source shapes used in this initial collection:

```
node scripts/import-campaign-evidence.mjs mailchimp new-mailchimp.json 2026-09-17T15:00:00Z
node scripts/import-campaign-evidence.mjs eventbrite new-eventbrite.json 2026-09-17T15:00:00Z
npm run intelligence:build
npm test
```

Mailchimp input: `campaigns` array using `campaign_id`, `category`, `category_evidence`, `subject`, `audience_name`, `send_time`, `days_before_event_local_date`, `delivered_estimate`, `unique_clickers`, `unsubscribed`, `cross_category_source_flag`, `report_content_collected`, `body_text`, `archive_url`. Eventbrite input: `records` array with `id`, `sender`, `event_name`, `subject`, `body`, `content_verified`, `audience_lists`, `send_label`, optional explicitly verified `verified_sent_at` and `verified_days_out`, `delivered`, `click_rate`, `unsubscribe_rate`, `bounce_rate`, `revenue`, `ticket_sales`, `note`. Eventbrite rates are percentages (4.2, not 0.042). Unknown fields must be omitted/null, never invented zeros. A partial report with no newly observed content should omit content fields.

Provider + campaign ID is the identity. A newer observation replaces known values, including zero and false; unobserved null values preserve prior evidence. Older observations cannot replace newer ones. Preserve the record's original observation time when retrying an import. Re-importing is idempotent. Review changes to the private source and generated report together. Never commit either dataset: this repository is public. Import corrections to campaign identity/category with explicit observed values; errors in retained fields can also be corrected in the private source registry. No automatic provider sync or scheduled browser scraper is claimed. The agent imports newly recovered evidence and deploys the changed report; the build script regenerates it from the private evidence. Store zlib-compressed, base64-encoded JSON in the existing Railway service variables CRAFT_CAMPAIGN_EVIDENCE_ZLIB_B64 (source) and CRAFT_CAMPAIGN_REPORT_ZLIB_B64 (report), without printing their contents. Deploy only after checking sync status. The authenticated GET /api/intelligence/campaign-history returns the report with no-store through the existing Vercel proxy; it never returns the source bundle. No campaign data is placed in frontend static assets. CI uses synthetic fixtures and does not need production evidence. A page refresh never advances the source date.

## Applying a recommendation

Learn message direction across a festival category; build recipients for the exact city, festival type and edition. Keep consent, suppression, recent-contact and current-buyer checks in existing preparation. No recommendation enables sending. The owner's approved super-spreader tone stays the baseline. Try one changed element against it; verify present-day lineup, inclusions, price deadlines and event dates. Seasonal hooks and city celebrations are conditional examples, not universal instructions. Persist future measured outcomes with their original provider attribution and refresh the comparison; do not present weak evidence as proven sales impact.
