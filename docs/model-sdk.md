# Maintained Claude transport and structured output

The existing campaign generator and learning extractor now use Anthropic's
[MIT-licensed Python SDK](https://github.com/anthropics/anthropic-sdk-python),
pinned to 1.6.0, and its documented
[structured-output helper](https://github.com/anthropics/anthropic-sdk-python/blob/main/examples/structured_outputs.py).
This replaces the hand-written requests client and regex/brace JSON recovery.
No agent framework, scheduler, tool loop, or new model call is introduced.

Craft retains its content contracts in Pydantic models. Campaigns require usable
subject/body/CTA fields and bounded estimate values. Learning responses have a
separate schema. These checks establish shape, not factual accuracy, consent,
causal evidence or authorization to send. Existing deterministic audience and
execution gates still apply; external sending stays disabled.

The SDK uses the existing model and token budgets, a 60-second timeout, and zero
automatic retries. A timeout can represent paid work of unknown outcome, so a
retry must be a deliberate later generation. Truncated/refused/invalid responses
return no result. There is no permissive parser fallback. Logs contain duration,
input/output token counts and exception class/numeric HTTP status, never raw
prompts, responses, credentials or provider error bodies. Token counts are
observed usage, not a price estimate or proof of campaign impact.

Tests exercise the installed SDK against an in-memory HTTP transport, including
valid campaigns, distinct learning output, invalid rates/fields, truncation and
429 responses. They never contact Anthropic or incur model charges. Run these
through the existing security suite in both database CI jobs. Local environments
without the SDK skip only the six real-SDK tests; CI installs dependencies.
