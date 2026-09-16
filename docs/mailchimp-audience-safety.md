# Festival audience safety

Craft Dominant serves multiple festivals. MAILCHIMP_AUDIENCE_ID is a legacy single-list setting, not a routing rule. New real executions require MAILCHIMP_EVENT_AUDIENCES, a JSON object mapping actual Eventbrite event IDs to verified Mailchimp API audience IDs. No name similarity, city match, or default audience authorizes a route. Unmapped events fail before campaign creation. Keep external sending disabled while these mappings are verified.

The authenticated /audiences page inventories the full Mailchimp account and identifies the legacy configured list. Inventory is read-only and includes no contact details. Truncated pagination fails rather than claiming all audiences were inspected.

Consent is specific to an audience. The provider preparation path checks existing subscribed members of the mapped destination. It never subscribes a ticket buyer or re-subscribes an unsubscribed contact. It creates a new attempt-specific static segment and verifies its complete membership against the computed audience. A missing segment ID cannot fall back to the whole audience.

Operational work still required:

- Verify each active festival's mapping against the audience's actual purpose, signup forms and historical campaign destination. Map timed-entry constituent events consistently.
- Refresh every mapped audience independently with POST /api/v2/suppressions/refresh and an audience_id in the JSON body. The scoped guard reuses the existing trust checks and never borrows freshness from another audience. Legacy global records are retained for compatibility but do not establish scope freshness. A webhook racing a refresh aborts that refresh so a recent unsubscribe cannot be erased.
- Install MAILCHIMP_WEBHOOK_SIGNING_SECRETS as a JSON object mapping audience IDs to their individual signing secrets, and test genuine signed deliveries for each audience. The untrusted payload list_id selects a key; that key must authenticate the entire body including the list identity. The legacy single signing key is not accepted in multi-audience mode.
- Verify purchase-history coverage before identifying inactivity candidates. No-click history alone is not permission to remove seasonal festival contacts.
- Review automations triggered by segment or tag changes before allowing provider preparation.

No contact deletions, unsubscribe operations, or campaign sends are authorized by this document. It records implementation and outstanding production gates; it is not evidence that operational setup is complete.
