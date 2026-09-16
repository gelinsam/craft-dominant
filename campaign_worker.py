"""On-demand durable draft worker. No network listener or external send operation."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
from urllib.parse import quote


def readonly_database(path):
    # Reuse the existing CRM query methods without invoking schema setup.
    from craft_unified import Database
    class ReadonlyCRM(Database):
        def __init__(self, path):
            self.path = path
            self.conn = sqlite3.connect('file:' + quote(str(Path(path).resolve())) + '?mode=ro', uri=True)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute('PRAGMA query_only=ON')
            # One consistent snapshot for profiles and edition-wide purchasers.
            self.conn.execute('BEGIN')
        def close(self):
            self.conn.close()
    return ReadonlyCRM(path)


def write_private(path, data):
    target=Path(path)
    # Never overwrite a previous package; operators inspect it before rerunning.
    fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as handle:
        json.dump(data,handle,indent=2)
        handle.write('\n')


def main():
    parser=argparse.ArgumentParser(description='Prepare a draft; never sends email')
    parser.add_argument('--input',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--analytics-db',default=os.environ.get('DB_PATH'))
    parser.add_argument('--workflow-db-url',default=os.environ.get('CRAFT_DRAFT_DATABASE_URL'))
    args=parser.parse_args()
    if not args.analytics_db or not args.workflow_db_url:
        parser.error('Explicit analytics and workflow databases are required')
    if Path(args.output).exists():
        parser.error('Output already exists; inspect it before running again')
    payload=json.loads(Path(args.input).read_text())
    from campaign_workflow import start_runtime,prepare_and_wait
    from dbos import DBOS
    start_runtime(lambda:readonly_database(args.analytics_db),args.workflow_db_url)
    try:
        result=prepare_and_wait(payload['request'],payload['sources'],payload['contact_history'])
        write_private(args.output,result)
        print(json.dumps({k:result[k] for k in ('run_id','state','recipient_count','sending_blockers')}))
    finally:
        DBOS.destroy()

if __name__=='__main__': main()
