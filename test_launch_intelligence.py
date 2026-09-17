import unittest
from launch_intelligence import summarize

class LaunchEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.e={'city':'Seattle','date':'2025-10-25','end_date':'2025-10-25'}
    def c(self,**kw):
        return {'account_id':'a','campaign':{'id':'1','name':'Seattle Coffee','objective':'OUTCOME_ENGAGEMENT'},'candidate_cities':['Seattle'],'days':[{'date_start':'2025-09-01','spend':'10'}],**kw}
    def test_missing_spend_is_unknown(self):
        self.assertIsNone(summarize(self.e,[],[],'2023-01-01')['spend'])
    def test_shared_city_campaign_is_excluded(self):
        self.assertIsNone(summarize(self.e,[],[self.c(candidate_cities=['Seattle','Dallas'])],'2023-01-01')['spend'])
    def test_campaign_duplicate_does_not_double_count(self):
        c=self.c();self.assertEqual(summarize(self.e,[],[c,c],'2023-01-01')['spend'],10)
    def test_wrong_edition_year_excluded(self):
        c=self.c(campaign={'id':'1','name':'Seattle Coffee 2026'})
        self.assertIsNone(summarize(self.e,[],[c],'2023-01-01')['spend'])
    def test_previous_edition_spend_excluded(self):
        e={**self.e,'previous_end':'2025-09-15'}
        self.assertIsNone(summarize(e,[],[self.c()],'2023-01-01')['spend'])
    def test_unknown_order_values_visible(self):
        s=[{'date':'2025-09-01','tickets':None,'revenue':None,'unknown_tickets':1,'unknown_revenue':1}]
        r=summarize(self.e,s,[],'2023-01-01');self.assertEqual(r['unknown_tickets'],1);self.assertTrue(r['warnings'])
    def test_unsafe_creative_link_not_exposed(self):
        c=self.c(ads=[{'creative':{'instagram_permalink_url':'https://evil.example/x'}}])
        self.assertIsNone(summarize(self.e,[],[c],'2023-01-01')['campaigns'][0]['creatives'][0]['url'])
    def test_retention_warning(self):
        self.assertTrue(summarize(self.e,[],[],'2025-09-01')['warnings'])

if __name__=='__main__':unittest.main()
