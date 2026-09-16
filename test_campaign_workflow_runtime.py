"""Real DBOS integration, run in isolated subprocesses and a temporary database."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

CHILD = r'''
import os
from pathlib import Path
from unittest.mock import Mock
from dbos import DBOS, SetWorkflowID
import campaign_workflow as runtime
from test_campaign_preparation import CampaignPreparationTests
case = CampaignPreparationTests(); case.setUp()
from datetime import datetime, timezone
stamp = datetime.now(timezone.utc).isoformat()
case.sources[0]['observed_at'] = stamp
case.history['observed_at'] = stamp
root=Path(os.environ['TEST_ROOT'])
def factory():
    with (root/'calls').open('a') as f: f.write('build\n')
    return case.db
@DBOS.workflow()
def crash_after_checkpoint():
    result=runtime.build_package(case.request,case.sources,case.history)
    marker=root/'crashed'
    if not marker.exists():
        marker.write_text('yes')
        os._exit(17)
    return result
runtime.start_runtime(factory,'sqlite:///'+str(root/'dbos.sqlite'))
try:
    with SetWorkflowID('crash-proof'):
        result=DBOS.start_workflow(crash_after_checkpoint).get_result()
    assert result['recipient_count']==2
    assert result['external_send_enabled'] is False
finally:
    DBOS.destroy()
'''

class WorkflowRuntimeTests(unittest.TestCase):
    def test_restart_recovers_checkpoint_without_rebuilding(self):
        with tempfile.TemporaryDirectory() as root:
            env=dict(os.environ,TEST_ROOT=root)
            first=subprocess.run([sys.executable,'-c',CHILD],env=env,capture_output=True,text=True,timeout=60)
            self.assertEqual(first.returncode,17,first.stderr)
            second=subprocess.run([sys.executable,'-c',CHILD],env=env,capture_output=True,text=True,timeout=60)
            self.assertEqual(second.returncode,0,second.stderr)
            self.assertEqual((Path(root)/'calls').read_text(),'build\n')

if __name__=='__main__': unittest.main()
