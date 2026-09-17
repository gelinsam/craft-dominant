from datetime import date
import unittest
from launch_action_packs import make_pack, asset_library, references

class TestLaunchActionPacks(unittest.TestCase):
    def edition(self,city='New York',**kw):
        e=dict(city=city,date='2026-10-03',end_date='2026-10-03',event_ids=['123'],event_days=1,lifecycle='relaunch',days_out=16,completed=False,tickets=80,spend=500,daily=[],warnings=[],instagram_references=[])
        e.update(kw);return e
    def assets(self):
        return [{'url':'https://www.instagram.com/seattlecoffeefestival/p/abc/','source_city':'Seattle','format':f,'description':'coffee'} for f in ['community','experience','countdown']]
    def test_launch_reuses_other_city_without_local_claims(self):
        p=make_pack(self.edition(),[],self.assets(),date(2026,9,17),'current')
        self.assertIn('do not assume',p['audience']);self.assertFalse(p['execution_allowed'])
        self.assertEqual(p['content'][0]['asset']['source_city'],'Seattle')
        self.assertIn('NYC Coffee Festival',p['content'][0]['caption'])
        self.assertNotIn('Seattle',p['content'][0]['caption'])
    def test_own_history_and_duration_match_take_precedence(self):
        target=self.edition('Seattle',lifecycle='returning')
        past=self.edition('Seattle',date='2025-10-03',end_date='2025-10-03',completed=True,tickets=200,daily=[{'date':'2025-09-01','tickets':100}])
        wrong=self.edition('Seattle',date='2024-10-03',completed=True,event_days=2,tickets=900)
        refs,basis=references(target,[past,wrong]);self.assertEqual(len(refs),1);self.assertIn('own',basis)
        p=make_pack(target,[past],self.assets(),date(2026,9,17),'current');self.assertEqual(p['pace_delta_pct'],-20)
        self.assertIn('conversion',p['priority']);self.assertIn('1–3 years',p['audience'])
    def test_more_spend_fewer_tickets_triggers_diagnosis(self):
        a=self.edition('Philadelphia',date='2024-10-03',completed=True,tickets=300,spend=100)
        b=self.edition('Philadelphia',date='2025-10-03',completed=True,tickets=200,spend=200)
        p=make_pack(self.edition('Philadelphia',lifecycle='returning'),[a,b],[],date(2026,9,17),'current')
        self.assertIn('conversion',p['priority']);self.assertTrue(any('spent more' in w for w in p['warnings']))
    def test_unknown_tickets_cannot_create_false_pacing(self):
        past=self.edition('Seattle',date='2025-10-03',completed=True,lifecycle='first launch',daily=[{'date':'2025-09-01','tickets':100}])
        p=make_pack(self.edition(tickets=None),[past],[],date(2026,9,17),'stale')
        self.assertIsNone(p['pace_delta_pct']);self.assertEqual(p['priority'],'Review sales evidence')
        self.assertTrue(p['content'][0]['blockers'])
    def test_stable_weekly_ids_and_phase_change(self):
        a=make_pack(self.edition(),[],[],date(2026,9,17),'current')
        b=make_pack(self.edition(),[],[],date(2026,9,18),'current')
        self.assertEqual(a['id'],b['id'])
        c=make_pack(self.edition(days_out=5),[],[],date(2026,9,28),'current')
        self.assertNotEqual(a['id'],c['id']);self.assertEqual(c['phase'],'final')
    def test_asset_urls_are_allowlisted_and_deduplicated(self):
        e=self.edition(instagram_references=[{'url':'javascript:alert(1)'},{'url':'https://www.instagram.com/dallascoffeefest/p/abc/','description':'save the date'}])
        assets=asset_library([e,e]);self.assertEqual(len(assets),1);self.assertEqual(assets[0]['format'],'announcement')
    def test_houston_does_not_invent_date_or_ticket_link(self):
        p=make_pack(self.edition('Houston',date=None,end_date=None,event_ids=[],lifecycle='planned launch',days_out=180),[],self.assets(),date(2026,9,17),'unknown')
        self.assertIsNone(p['event_date']);self.assertEqual(p['ticket_links'],[])
        self.assertIn('Date to be confirmed',p['content'][0]['caption'])
    def test_other_coffee_cities_group_sessions_and_exclude_vendor(self):
        import sqlite3
        from types import SimpleNamespace
        from launch_action_packs import complete_editions
        con=sqlite3.connect(':memory:');self.addCleanup(con.close);con.row_factory=sqlite3.Row
        con.execute('CREATE TABLE orders(event_id TEXT,order_timestamp TEXT,ticket_count INTEGER,gross_amount REAL)')
        con.execute("INSERT INTO orders VALUES('1','2026-09-01',3,30)")
        events=[{'event_id':str(i),'name':'Austin Coffee Festival' if i<3 else 'Coffee exhibitor','event_type':'coffee','city':'Austin','event_date':'2026-10-0'+str(i),'status':'active'} for i in (1,2,3)]
        db=SimpleNamespace(conn=con,get_events=lambda:events,edition_sibling_ids=lambda i:['1','2'] if i in ('1','2') else ['3'])
        editions=complete_editions({'editions':[]},db,date(2026,9,17))
        self.assertEqual(len(editions),1);self.assertEqual(editions[0]['tickets'],3)
        self.assertEqual(editions[0]['event_days'],2);self.assertIsNone(editions[0]['spend'])
    def test_crm_reuses_filter_and_only_exposes_counts(self):
        from unittest.mock import Mock,patch
        from types import SimpleNamespace
        import sys,json
        from datetime import datetime,timezone
        from launch_action_packs import crm_counts
        builder=Mock(return_value={'records':[{'email':'private@example.test'}],'excluded_current_buyers':2})
        with patch.dict(sys.modules,{'crm_audience':SimpleNamespace(build_crm_audience=builder)}):
            counts=crm_counts(None,{'event_ids':['1'],'has_prior_customers':True},datetime.now(timezone.utc))
        self.assertEqual(len(counts),3);self.assertEqual(counts[0]['candidates'],1)
        self.assertNotIn('private@example',json.dumps(counts))
        self.assertEqual(builder.call_args.args[3],'ticket_sales')
