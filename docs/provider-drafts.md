# Provider drafts

The Actions view leads with actual recipient-bound Mailchimp campaigns. Analysis is supporting detail. A recommendation is not counted as completed work until a saved provider draft has its content, exact recipient segment and Mailchimp checklist verified.

This extends the existing Mailchimp client, CRM edition resolver, private atomic snapshots and campaign maintenance worker. No new agent framework, email service or scheduling infrastructure is introduced. Mailchimp's own campaign editor remains the review/send interface.

Private `/data/provider-draft-briefs.json` contains reviewed festival scope, copy, ticket destination and explicit audience purpose. The existing verified event-to-list mapping is mandatory. The current configuration is an editorial input, not a claim that arbitrary future recommendations are automatically supported. Add reviewed briefs as category evidence and event facts change.

The worker checks actual current Eventbrite orders across all edition sessions, destination Mailchimp subscription eligibility, recent same-provider contact and scheduled campaigns. It never merges city audiences or subscribes contacts. Cross-provider attempts do not remove a recipient. Scheduled campaigns are timing context and do not exclude recipients from the next draft; the owner chooses when to send. Existing scheduled campaigns are never changed. Current order reads are independent of the long historical sync and make no database mutations.

A stable festival/segment/idea key reconciles creation. A pending create is saved before the request; uncertain results are inspected rather than retried blindly. Saved owner edits and scheduled/sending campaigns are preserved. Drafts refresh through the existing maintenance thread every 15 minutes. The UI expires ready status after one hour if verification stops. No send, test-send or scheduling operation exists in this module.

Private state is excluded from the public repository. The read endpoint is authenticated and no-store, returns aggregate recipient counts and campaign links, and omits recipient records and fingerprints. Review new audience automations before enabling preparations that could trigger them.
