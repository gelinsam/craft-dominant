"""Remove only byte-for-byte duplicate spend on the wrong edition, with archive.

No Meta calls, ticket writes or guessed reassignment. Caller supplies audited
source/keeper IDs. Dry run by default; conflicting observations stop the repair.
"""
import argparse
import json
import os
from craft_unified import Database, festival_edition_key


def repair(db, source, keeper, apply=False):
    events = {r['event_id']: dict(r) for r in db.conn.execute(
        'SELECT event_id,name,event_date FROM events WHERE event_id IN (?,?)', (source, keeper))}
    if len(events) != 2 or festival_edition_key(events[source]['name'], events[source]['event_date']) == festival_edition_key(events[keeper]['name'], events[keeper]['event_date']):
        raise ValueError('Repair requires two verified distinct editions')
    # Limit to exact campaign/date duplicates. Preserve newly legitimate spend.
    query = '''SELECT s.* FROM ad_spend s JOIN ad_spend k
        ON k.campaign_id=s.campaign_id AND k.spend_date=s.spend_date
        WHERE s.event_id=? AND k.event_id=?'''
    rows = [dict(r) for r in db.conn.execute(query, (source, keeper))]
    for r in rows:
        k = db.conn.execute('SELECT * FROM ad_spend WHERE event_id=? AND campaign_id=? AND spend_date=?',
                            (keeper,r['campaign_id'],r['spend_date'])).fetchone()
        if any(r[field] != k[field] for field in ('spend','impressions','clicks')):
            raise ValueError('Conflicting duplicate observations; manual review required')
    result = {'source': source, 'keeper': keeper, 'rows': len(rows),
              'campaigns': sorted({r['campaign_id'] for r in rows}),
              'duplicate_spend': round(sum(r['spend'] or 0 for r in rows),2), 'applied': apply}
    if apply and rows:
        with db.transaction() as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS edition_spend_repair_archive AS SELECT * FROM ad_spend WHERE 0')
            conn.execute('CREATE TABLE IF NOT EXISTS edition_snapshot_repair_archive AS SELECT * FROM daily_snapshots WHERE 0')
            conn.execute('INSERT INTO edition_snapshot_repair_archive SELECT * FROM daily_snapshots WHERE event_id=?', (source,))
            for r in rows:
                # Recheck under the write transaction; stop if a sync changed it.
                current = conn.execute('SELECT * FROM ad_spend WHERE id=?', (r['id'],)).fetchone()
                if not current or dict(current) != r:
                    raise ValueError('Spend changed during repair; retry from fresh evidence')
                conn.execute('INSERT INTO edition_spend_repair_archive SELECT * FROM ad_spend WHERE id=?', (r['id'],))
                conn.execute('DELETE FROM ad_spend WHERE id=?', (r['id'],))
            # Refresh only affected spend snapshots; all sales fields untouched.
            conn.execute('''UPDATE daily_snapshots SET ad_spend_cumulative=(
                SELECT COALESCE(SUM(a.spend),0) FROM ad_spend a
                WHERE a.event_id=daily_snapshots.event_id AND a.spend_date<=daily_snapshots.snapshot_date)
                WHERE event_id=?''', (source,))
    return result


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',required=True); p.add_argument('--keeper',required=True)
    p.add_argument('--apply',action='store_true')
    a=p.parse_args()
    print(json.dumps(repair(Database(os.environ.get('DB_PATH','craft_unified.db')),a.source,a.keeper,a.apply)))
