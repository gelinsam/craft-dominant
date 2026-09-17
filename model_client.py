"""Anthropic SDK boundary: typed output, one request, no raw payload logging."""
import logging
import time

log = logging.getLogger(__name__)


class ClaudeClient:
    def __init__(self, api_key, model='claude-sonnet-5'):
        self.api_key = api_key
        self.model = model

    def _request(self, system_prompt, user_prompt, max_tokens, output_kind=None):
        started = time.monotonic()
        try:
            from anthropic import Anthropic
            # Preserve the existing one-attempt policy and 60-second timeout.
            # Automatic retry after an uncertain timeout can duplicate paid work.
            with Anthropic(api_key=self.api_key, timeout=60.0, max_retries=0) as client:
                kwargs = dict(model=self.model, max_tokens=max_tokens,
                              system=system_prompt,
                              messages=[{'role':'user', 'content':user_prompt}])
                if output_kind:
                    from model_outputs import CampaignOutput, LearningOutput
                    schemas = {'campaign':CampaignOutput, 'learning':LearningOutput}
                    if output_kind not in schemas:
                        raise ValueError('Unsupported output schema')
                    message = client.messages.parse(output_format=schemas[output_kind], **kwargs)
                else:
                    message = client.messages.create(**kwargs)
            usage = getattr(message, 'usage', None)
            log.info('Claude request finished duration_ms=%s input_tokens=%s output_tokens=%s',
                     round((time.monotonic()-started)*1000),
                     getattr(usage, 'input_tokens', None), getattr(usage, 'output_tokens', None))
            # Refusals, truncation and tool requests cannot become draft content.
            if getattr(message, 'stop_reason', None) != 'end_turn':
                log.warning('Claude output was not a completed response')
                return None
            if output_kind:
                parsed = getattr(message, 'parsed_output', None)
                if not isinstance(parsed, schemas[output_kind]):
                    log.warning('Claude output did not validate against the requested schema')
                    return None
                return parsed.model_dump()
            content = ''.join(block.text for block in message.content if block.type == 'text')
            return content or None
        except Exception as exc:
            # Provider exception bodies and generated text can contain customer
            # context. Log only the exception class and a numeric HTTP status.
            status = getattr(exc, 'status_code', None)
            log.error('Claude request failed error_type=%s http_status=%s duration_ms=%s',
                      type(exc).__name__, status if isinstance(status, int) else None,
                      round((time.monotonic()-started)*1000))
            return None

    def generate(self, system_prompt, user_prompt, max_tokens=4000, temperature=0.7):
        """Legacy temperature argument stays accepted but is never transmitted."""
        return self._request(system_prompt, user_prompt, max_tokens)

    def generate_json(self, system_prompt, user_prompt, max_tokens=4000,
                      temperature=0.5, output_kind='campaign'):
        """Return a validated dictionary; never salvage JSON from prose."""
        return self._request(system_prompt, user_prompt, max_tokens, output_kind)
