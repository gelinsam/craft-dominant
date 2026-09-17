"""SDK contract tests use a fake HTTP provider, never a paid model request."""
import importlib.util
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, MagicMock, patch
from model_client import ClaudeClient


class TestModelBoundary(unittest.TestCase):
    def test_sdk_failure_never_logs_private_payload(self):
        sdk = Mock()
        sdk.Anthropic.side_effect = RuntimeError('PRIVATE-CUSTOMER-CONTENT secret-token')
        with patch.dict('sys.modules', {'anthropic':sdk}), self.assertLogs('model_client', level='ERROR') as logs:
            self.assertIsNone(ClaudeClient('private-key').generate('system', 'customer'))
        self.assertNotIn('PRIVATE-CUSTOMER', str(logs.output))
        self.assertNotIn('secret-token', str(logs.output))
        self.assertNotIn('private-key', str(logs.output))
        sdk.Anthropic.assert_called_once_with(api_key='private-key',timeout=60.0,max_retries=0)

    def test_truncated_text_never_becomes_content(self):
        sdk = MagicMock(); client=sdk.Anthropic.return_value.__enter__.return_value
        client.messages.create.return_value=SimpleNamespace(stop_reason='max_tokens',usage=None,content=[SimpleNamespace(type='text',text='partial')])
        with patch.dict('sys.modules', {'anthropic':sdk}):
            self.assertIsNone(ClaudeClient('test').generate('system','prompt'))
        self.assertNotIn('temperature',client.messages.create.call_args.kwargs)


@unittest.skipUnless(importlib.util.find_spec('anthropic') and importlib.util.find_spec('pydantic'), 'SDK dependencies installed in CI')
class TestStructuredSDK(unittest.TestCase):
    def campaign(self):
        return dict(subject_line='Bring your crew', preview_text='Coffee together',body_html='<p>Join us.</p>',
                    cta_text='Tickets',cta_url='https://www.eventbrite.com/',barrier_addressed='social',
                    strategic_reasoning='Invite a friend.', predicted_open_rate=.2,predicted_click_rate=.03,
                    confidence_score=.5,segment_priority='Group buyers')

    def request(self, output, status=200, stop='end_turn', kind='campaign'):
        import anthropic, httpx2
        self.requests=[]
        def respond(request):
            self.requests.append(json.loads(request.content))
            payload={'id':'msg_test','type':'message','role':'assistant','model':'claude-sonnet-5',
                     'content':[{'type':'text','text':json.dumps(output)}],
                     'stop_reason':stop,'stop_sequence':None,'usage':{'input_tokens':10,'output_tokens':20}}
            if status != 200:
                payload={'type':'error','error':{'type':'rate_limit_error','message':'PRIVATE-PROVIDER-BODY'}}
            return httpx2.Response(status,json=payload)
        real=anthropic.Anthropic(api_key='test-only',max_retries=0,timeout=60,
            http_client=httpx2.Client(transport=httpx2.MockTransport(respond)))
        with patch('anthropic.Anthropic',return_value=real):
            return ClaudeClient('test-only').generate_json('system','prompt',output_kind=kind)

    def test_actual_sdk_parses_campaign_schema(self):
        expected=self.campaign()
        self.assertEqual(self.request(expected),expected)
        self.assertEqual(len(self.requests),1)
        self.assertNotIn('temperature',self.requests[0])
        self.assertIn('output_config',self.requests[0])

    def test_actual_sdk_rejects_missing_required_content(self):
        data=self.campaign();del data['body_html']
        self.assertIsNone(self.request(data))

    def test_actual_sdk_rejects_invalid_rate_and_extra_fields(self):
        for changes in ({'predicted_open_rate':1.5},{'unexpected':'value'},{'body_html':''}):
            with self.subTest(changes=changes):
                self.assertIsNone(self.request(dict(self.campaign(),**changes)))

    def test_actual_sdk_learning_schema_is_distinct(self):
        data={'learnings':[{'category':'copy','learning':'Use concrete details.','confidence':.4},
                           {'category':'timing','learning':'Test an earlier draft.','confidence':.3}],
              'what_worked':'Specific copy.','what_to_improve':'Collect more evidence.'}
        self.assertEqual(self.request(data,kind='learning'),data)
        self.assertIsNone(self.request(self.campaign(),kind='learning'))

    def test_rate_limit_is_single_attempt_and_redacted(self):
        with self.assertLogs('model_client',level='ERROR') as logs:
            self.assertIsNone(self.request({},status=429))
        self.assertEqual(len(self.requests),1)
        self.assertNotIn('PRIVATE-PROVIDER',str(logs.output))

    def test_truncated_structured_output_is_rejected(self):
        self.assertIsNone(self.request(self.campaign(),stop='max_tokens'))
