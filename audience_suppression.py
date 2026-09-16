"""Audience-scoped storage using the existing suppression trust validator.

The validation policy remains SuppressionGuard's. Scoped email rows and their
sentinels live together on the mounted analytics database; no copied validation
state machine or new infrastructure dependency is introduced.
"""
import json
import os
import re
from suppression_guard import SuppressionGuard
from audience_routing import event_audience_id


class AudienceSuppressionGuard(SuppressionGuard):
    def __init__(self, db, audience_id):
        if not isinstance(audience_id, str) or not re.fullmatch(r'[a-fA-F0-9]{10}', audience_id):
            raise ValueError('Invalid Mailchimp audience ID')
        self.db = db
        self.audience_id = audience_id
        self._v2_repo = None
        with db.transaction() as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS audience_suppressions (
                audience_id TEXT NOT NULL, email TEXT NOT NULL, reason TEXT NOT NULL,
                PRIMARY KEY (audience_id, email))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS audience_suppression_sync (
                audience_id TEXT PRIMARY KEY, sentinel_json TEXT NOT NULL)''')

    def _actual_suppression_count(self):
        return self.db.conn.execute(
            'SELECT COUNT(*) FROM audience_suppressions WHERE audience_id=?',
            (self.audience_id,)).fetchone()[0]

    def _read_suppression_emails(self):
        return self.db.conn.execute(
            'SELECT email FROM audience_suppressions WHERE audience_id=?',
            (self.audience_id,)).fetchall()

    def _read_sentinel(self):
        row = self.db.conn.execute(
            'SELECT sentinel_json FROM audience_suppression_sync WHERE audience_id=?',
            (self.audience_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def _write_sentinel(self, data):
        with self.db.transaction() as conn:
            merged = self._read_sentinel() or {}
            merged.update(data)
            conn.execute('''INSERT INTO audience_suppression_sync VALUES (?, ?)
                ON CONFLICT(audience_id) DO UPDATE SET sentinel_json=excluded.sentinel_json''',
                (self.audience_id, json.dumps(merged)))

    def _replace_suppression_emails(self, emails):
        with self.db.transaction() as conn:
            current_revision = (self._read_sentinel() or {}).get('mutation_sequence', 0)
            if current_revision != self._refresh_revision:
                raise RuntimeError('Webhook arrived during refresh; retry authoritative reconciliation')
            conn.execute('DELETE FROM audience_suppressions WHERE audience_id=?', (self.audience_id,))
            conn.executemany('INSERT INTO audience_suppressions VALUES (?, ?, ?)',
                [(self.audience_id, email, 'mailchimp_suppressed') for email in emails])

    def refresh_from_mailchimp(self, client):
        if getattr(client, 'audience_id', None) != self.audience_id:
            return {'error':'Provider audience does not match suppression scope'}
        self._refresh_revision = (self._read_sentinel() or {}).get('mutation_sequence', 0)
        return super().refresh_from_mailchimp(client)

    def record_email(self, email, reason, source='webhook'):
        with self.db.transaction() as conn:
            conn.execute('''INSERT INTO audience_suppressions VALUES (?, ?, ?)
                ON CONFLICT(audience_id,email) DO UPDATE SET reason=excluded.reason''',
                (self.audience_id, email.lower().strip(), reason))
            sentinel = self._read_sentinel() or {}
            sentinel['mutation_sequence'] = sentinel.get('mutation_sequence', 0) + 1
            conn.execute("""INSERT INTO audience_suppression_sync VALUES (?, ?)
                ON CONFLICT(audience_id) DO UPDATE SET sentinel_json=excluded.sentinel_json""",
                (self.audience_id, json.dumps(sentinel)))
        return self.record_mutation(source=source)

    def acknowledge_empty(self, actor, reason):
        return {'error':'Empty audience scopes require separate verification; sending remains blocked'}


def guard_for_events(db, event_ids, legacy_guard):
    if not os.environ.get('MAILCHIMP_EVENT_AUDIENCES'):
        return legacy_guard
    destinations = {event_audience_id(event_id) for event_id in event_ids}
    if len(destinations) != 1:
        raise RuntimeError('Grouped festival events must share one verified Mailchimp audience')
    return AudienceSuppressionGuard(db, destinations.pop())
