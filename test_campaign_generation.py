"""Automatic generation reuses festival editions, not raw session rows."""
from datetime import date, timedelta
from unittest import TestCase
from unittest.mock import Mock
from craft_unified import Database
from craft_engine import CraftCampaignEngine


class CampaignEditionGenerationTests(TestCase):
    def setUp(self):
        self.db=Database(':memory:')
        self.engine=CraftCampaignEngine(self.db)
        self.today=date.today()

    def add(self,eid,name,days,status='live'):
        self.db.conn.execute('INSERT INTO events (event_id,name,event_date,status) VALUES (?,?,?,?)',
            (eid,name,(self.today+timedelta(days=days)).isoformat(),status))
        self.db.conn.commit()

    def test_sessions_generate_one_opportunity_and_reuse_existing_sibling_work(self):
        self.add('101','Austin Coffee Festival - Saturday',20)
        self.add('102','Austin Coffee Festival - Sunday',21)
        items=self.engine.detect_phases()
        self.assertEqual(len(items),1)
        self.assertEqual(items[0]['event_ids'],['101','102'])
        self.assertEqual(items[0]['event']['event_id'],'101')
        self.db.conn.execute("INSERT INTO phase_log(event_id,phase) VALUES ('102','urgency')")
        self.assertEqual(self.engine.detect_phases(),[])

    def test_cities_and_seasons_remain_separate(self):
        self.add('101','Philly Wine Fest! Spring Edition',20)
        self.add('102','Philly Wine Fest! Fall Edition',21)
        self.add('103','DC Wine Fest',22)
        self.assertEqual(len(self.engine.detect_phases()),3)

    def test_exhibitors_and_cancelled_events_are_excluded(self):
        self.add('101','Austin Coffee Festival Exhibitor Payment',20)
        self.add('102','DC Coffee Festival',20,'canceled')
        self.assertEqual(self.engine.detect_phases(),[])

    def test_post_event_waits_until_the_entire_edition_ends(self):
        self.add('101','Austin Coffee Festival - Saturday',-1)
        self.add('102','Austin Coffee Festival - Sunday',0)
        self.assertEqual(self.engine.detect_phases(),[])
        self.db.conn.execute('UPDATE events SET event_date=? WHERE event_id=?',
            ((self.today-timedelta(days=2)).isoformat(),'101'))
        self.db.conn.execute('UPDATE events SET event_date=? WHERE event_id=?',
            ((self.today-timedelta(days=1)).isoformat(),'102'))
        items=self.engine.detect_phases()
        self.assertEqual(len(items),1)
        self.assertEqual(items[0]['days_until'],-1)
        self.assertEqual(items[0]['phase']['name'],'post_event')

    def test_running_refresh_prevents_paid_generation(self):
        self.add('101','Austin Coffee Festival',20)
        self.db.last_sync_run=Mock(return_value={'status':'running'})
        self.engine.generate_campaign=Mock()
        self.assertEqual(self.engine.run_cycle(),[])
        self.engine.generate_campaign.assert_not_called()
