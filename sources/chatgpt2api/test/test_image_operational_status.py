import unittest,json
from services.image_operational_status import summarize,pool_summary
class StatusTests(unittest.TestCase):
 def row(self,status,ts,count=1,kind="generate"):
  return {"status":status,"updated_at":ts,"created_at":ts-10,"task_type":kind,"result_json":json.dumps({"count":count}),"error":"403 secret"}
 def test_counts_images_not_requests_and_excludes_pending(self):
  now=1792064000
  d=summarize([self.row("success",now-20,3),self.row("success",now-10,2,"edit"),self.row("error",now-5),self.row("running",now)],now)
  self.assertEqual(d["today"],{"generate":3,"edit":2})
  self.assertEqual(d["recent_count"],3)
  self.assertEqual(d["success_rate"],66.7)
  self.assertEqual(d["median_seconds"],10)
  self.assertNotIn("secret",str(d))
 def test_limits_to_recent_20_finished_in_24_hours(self):
  now=1792064000
  rows=[self.row("success",now-i) for i in range(25)]+[self.row("error",now-90000)]
  self.assertEqual(summarize(rows,now)["recent_count"],20)
 def test_stale_success_is_unverified(self):
  self.assertEqual(summarize([self.row("success",1000)],4000)["state"],"unverified")
 def test_empty_history_has_no_fake_percentage(self):
  d=summarize([],1000);self.assertIsNone(d["success_rate"]);self.assertIsNone(d["median_seconds"])
 def test_model_specific_cooldown_and_unknown_source(self):
  a={"id":1,"status":"active","schedulable":True,"extra":{"model_rate_limits":{"gpt-image-2.5-flare":{"rate_limit_reset_at":"2030-01-01T00:00:00Z"}},"codex_7d_used_percent":13}}
  d=pool_summary([a],"gpt-image-2.5-flare",1792064000)
  self.assertEqual(d["available"],0);self.assertEqual(d["cooling"],1)
  self.assertEqual(d["accounts"][0]["cooldown_source"],"unknown")
  self.assertEqual(d["accounts"][0]["windows"][0]["remaining"],87)
  self.assertEqual(pool_summary([a],"gpt-image-2",1792064000)["available"],1)
 def test_disabled_account_never_available(self):
  self.assertEqual(pool_summary([{"id":1,"status":"disabled","schedulable":True}],"gpt-image-2",1792064000)["available"],0)

 def test_collapsed_database_creation_uses_original_metadata_time(self):
  r=self.row("success",1792064000)
  r["created_at"]=r["updated_at"]
  r["metadata_json"]=json.dumps({"created_ts":1792063980})
  self.assertEqual(summarize([r],1792064001)["median_seconds"],20)
 def test_expired_usage_window_marked_stale(self):
  a={"id":1,"status":"active","schedulable":True,"extra":{"codex_5h_used_percent":0,"codex_5h_reset_at":1000}}
  self.assertTrue(pool_summary([a],"gpt-image-2",2000)["accounts"][0]["windows"][0]["stale"])
