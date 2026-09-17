from datetime import datetime, timezone
import copy
import os
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import provider_drafts as p
CURRENT_BUYERS=p.current_buyers

NOW=datetime.now(timezone.utc)
BRIEF={'event_id':'e','city':'City','event_type':'coffee','festival':'City Coffee', 'segment':'past_attendees','idea_id':'discovery-v1','purpose_label':'Past buyers', 'subject':'Discover coffee together','preview':'Your next coffee day','heading':'A new favorite','paragraphs':['Bring a friend.'], 'ticket_url':'https://www.eventbrite.com/e/123','rationale':'Evidence-based test'}

class Client:
    audience_id='1234567890';dc='us16';from_email='sender@example.com'
    def __init__(self):
        self.calls=[];self.campaign=None;self.content={};self.created=0
    def _request_strict(self,method,path,data=None,timeout=None):
        self.calls.append((method,path,data))
        if path.startswith('/campaigns?'):
            rows=[copy.deepcopy(self.campaign)] if self.campaign else []
            body={'campaigns':rows,'total_items':len(rows)}
        elif path.startswith('/automations?'): body={'automations':[],'total_items':0}
        elif '/members?' in path:
            body={'members':[{'id':'a','email_address':'a@example.com','status':'subscribed'}],'total_items':1}
        elif path=='/lists/1234567890': body={'campaign_defaults':{'from_name':'Festival','from_email':'sender@example.com'}}
        elif path.endswith('/content'):
            if method=='PUT': self.content=data
            body=self.content
        elif path.endswith('/send-checklist'): body={'is_ready':True}
        elif path=='/campaigns/c':
            if method=='PATCH':
                settings=data.pop('settings',{});self.campaign['settings'].update(settings)
                self.campaign.update(data);self.campaign['recipients']['recipient_count']=1
            body=copy.deepcopy(self.campaign)
        else: raise AssertionError(path)
        return SimpleNamespace(body=body)
    def ensure_members(self,emails): pass
    def tag_members(self,emails,tag): self.calls.append(('SEGMENT',tag,emails))
    def get_tag_segment_id(self,tag):return 2
    def _get_members_by_status(self,status):return ['a@example.com']
    def create_campaign_strict(self,subject,preview,body,segment_id,campaign_title):
        self.created+=1
        self.campaign={'id':'c','web_id':123,'status':'save','settings':{'title':campaign_title,'subject_line':subject},'recipients':{'list_id':self.audience_id,'segment_opts':{'saved_segment_id':segment_id},'recipient_count':1}}
        self.content={'html':body}
        return SimpleNamespace(provider_campaign_id='c',content_set=True)

class ProviderDraftTests(unittest.TestCase):
    def setUp(self):
        self.client=Client();self.state={};self.saved=[]
        self.db=SimpleNamespace(get_event=lambda e:{'name':'City Coffee','city':'City','event_type':'coffee','event_date':NOW.date().isoformat()},edition_sibling_ids=lambda e:['e'],last_sync_run=lambda:{'status':'completed'})
        for target,value in [('provider_drafts.event_audience_id','1234567890'),('provider_drafts.build_crm_audience',{'records':[{'email':'a@example.com'},{'email':'nonconsented@example.com'}]}),('provider_drafts.current_buyers',set())]:
            m=patch(target,return_value=value);m.start();self.addCleanup(m.stop)
    def prepare(self):return p.prepare_one(self.db,BRIEF,self.client,self.state,lambda:self.saved.append(copy.deepcopy(self.state)),NOW)
    def test_creates_exact_consent_segment_and_saved_draft_once(self):
        result=self.prepare();self.assertEqual(result['state'],'ready');self.assertEqual(result['audience_count'],1)
        self.assertEqual(self.client.calls[[x[0] for x in self.client.calls].index('SEGMENT')][2],['a@example.com'])
        self.prepare();self.assertEqual(self.client.created,1)
        self.assertFalse(any('/actions/' in x[1] for x in self.client.calls))
    def test_scheduled_campaign_never_changed(self):
        self.prepare();self.client.campaign['status']='schedule';self.client.calls=[]
        self.assertEqual(self.prepare()['state'],'schedule')
        self.assertTrue(all(x[0]=='GET' for x in self.client.calls))
    def test_owner_edits_preserved(self):
        self.prepare();self.client.content['html']='Owner changed this';self.client.calls=[]
        self.assertEqual(self.prepare()['state'],'user_edited')
        self.assertTrue(all(x[0]=='GET' for x in self.client.calls))
    def test_incomplete_sales_never_creates(self):
        with patch('provider_drafts.current_buyers',side_effect=ValueError('Current buyer verification unavailable')):
            with self.assertRaisesRegex(ValueError,'buyer verification'):self.prepare()
        self.assertEqual(self.client.created,0)
    def test_uncertain_create_cannot_retry_blindly(self):
        self.prepare();entry=next(iter(self.state['drafts'].values()));entry.pop('campaign_id');entry['create_pending']=True;self.client.campaign=None
        with self.assertRaisesRegex(ValueError,'uncertain'):self.prepare()
        self.assertEqual(self.client.created,1)
    def test_consent_changes_stop_campaign_creation(self):
        self.client._get_members_by_status=lambda status:[]
        with self.assertRaisesRegex(ValueError,'consent changed'):self.prepare()
        self.assertEqual(self.client.created,0)
    def test_missing_audience_does_not_fallback_to_whole_list(self):
        with patch('provider_drafts.build_crm_audience',return_value={'records':[]}):self.assertEqual(self.prepare()['state'],'no_audience')
        self.assertEqual(self.client.created,0)
    def test_pagination_truncation_rejected(self):
        c=SimpleNamespace(_request_strict=lambda *a,**k:SimpleNamespace(body={'members':[],'total_items':3}))
        with self.assertRaises(ValueError):p.page_all(c,'/members','members')
    def test_links_owner_scheduled_work_without_adopting_or_changing_it(self):
        self.client.campaign={'id':'c','web_id':123,'status':'schedule','settings':{'title':'Owner campaign','subject_line':'Owner subject'},'recipients':{'list_id':self.client.audience_id}}
        self.client.content={'html':'<a href="https://www.eventbrite.com/e/festival-123">Tickets</a>'}
        result=self.prepare()
        self.assertEqual(result['state'],'covered_by_scheduled')
        self.assertNotIn('campaign_id',result)
        self.assertTrue(all(c[0]=='GET' for c in self.client.calls))

    def test_live_buyer_is_removed_even_if_crm_snapshot_is_older(self):
        with patch('provider_drafts.current_buyers',return_value={'a@example.com'}):
            self.assertEqual(self.prepare()['state'],'no_audience')
        self.assertEqual(self.client.created,0)
    def test_live_buyer_pagination_and_identity_fail_closed(self):
        from unittest.mock import Mock
        source=Mock()
        source._paginate.return_value=[{'id':'o','email':' A@example.com '}]
        module=SimpleNamespace(EventbriteSync=lambda *args:source)
        with patch.dict('sys.modules',{'craft_unified':module}),patch.dict(os.environ,{'EVENTBRITE_API_KEY':'test'}):
            self.assertEqual(CURRENT_BUYERS(self.db,['e']),{'a@example.com'})
            self.assertTrue(source._paginate.call_args.kwargs['require_complete'])
            for rows in [[{'id':'o'}],[{'id':'o','email':'a@example.com'}]*2]:
                source._paginate.return_value=rows
                with self.assertRaises(ValueError):CURRENT_BUYERS(self.db,['e'])
            source._paginate.side_effect=RuntimeError('Pagination failed')
            with self.assertRaises(RuntimeError):CURRENT_BUYERS(self.db,['e'])

    def test_public_view_omits_private_state_and_expires_ready(self):
        self.prepare();entry=next(iter(self.state['drafts'].values()));entry['verified_at']='2020-01-01T00:00:00+00:00'
        with patch('provider_drafts.read_json',return_value=self.state):result=p.read_provider_drafts(NOW)['drafts'][0]
        self.assertEqual(result['state'],'refreshing');self.assertNotIn('recipient_digest',result)

if __name__=='__main__':unittest.main()
