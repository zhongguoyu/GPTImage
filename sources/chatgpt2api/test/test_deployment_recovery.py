import unittest
from dataclasses import replace
from test.test_scheduler_account_pool import make_accounts,make_settings,fake_router
from services.scheduler.account_pool import AccountPool
from services.scheduler.models import AccountRequirement,Lane,Outcome
from utils.helper import ensure_ok
class Response:
 status_code=404
 text=""
 headers={"Content-Type":"application/json"}
 def json(self): raise ValueError()
 def iter_content(self): yield b'{"detail":"Model not found"}'
class RecoveryTests(unittest.TestCase):
 def test_failed_account_does_not_outrank_successful_slow_image_account(self):
  pool=AccountPool(make_accounts("bad","good"),make_settings(),model_router=fake_router)
  req=AccountRequirement(Lane.IMAGE_GENERATION,"gpt-image-2",source_type="web",cost=1)
  good=pool.try_acquire(req,deadline=pool.clock()+5,excluded={"bad"})
  pool.release(good,outcome=Outcome.SUCCESS,latency_ms=20687)
  bad=pool.try_acquire(req,deadline=pool.clock()+5,excluded={"good"})
  pool.release(bad,outcome=Outcome.SERVER_ERROR,latency_ms=100)
  selected=pool.try_acquire(req,deadline=pool.clock()+5)
  self.assertEqual(selected.account_key,"good")
 def test_stream_error_retains_model_not_found(self):
  with self.assertRaisesRegex(Exception,"Model not found"):ensure_ok(Response(),"conversation")
 def test_challenge_has_actionable_error_without_html(self):
  r=Response();r.status_code=403;r.text="<html>challenge-secret</html>";r.headers={"cf-mitigated":"challenge"}
  with self.assertRaisesRegex(Exception,"Cloudflare") as ctx:ensure_ok(r,"bootstrap")
  self.assertNotIn("challenge-secret",str(ctx.exception))
