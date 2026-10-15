import io
import json
import os
import unittest
from unittest.mock import patch
from fastapi import HTTPException
from services import sub2_gateway as gateway
from services.protocol import openai_v1_chat_complete as chat, openai_v1_response as responses, anthropic_v1_messages as messages, openai_v1_models as models, openai_search

class EventResponse(io.BytesIO):
    headers={"Content-Type":"text/event-stream"}

class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{"SUB2API_ONLY":"1","IMAGE_UPSTREAM_CONFIG":"/configured"});self.env.start();self.addCleanup(self.env.stop)
        self.settings=patch.object(gateway,"settings",return_value={"base_url":"http://sub2api:8080","api_key":"test-key"});self.settings.start();self.addCleanup(self.settings.stop)

    def test_chat_preserves_tools_and_vision(self):
        body={"model":"auto","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}],"tools":[{"type":"function","function":{"name":"test","parameters":{}}}],"stream":False}
        with patch.object(gateway,"json_request",return_value={"choices":[]}) as send:
            chat.handle(body)
        path,payload=send.call_args.args
        self.assertEqual(path,"/v1/chat/completions");self.assertEqual(payload['model'],'gpt-5.6')
        self.assertEqual(payload['tools'],body['tools']);self.assertEqual(payload['messages'],body['messages'])

    def test_responses_preserves_tools(self):
        body={"model":"gpt-6","input":"hello","tools":[{"type":"function","name":"read_file","parameters":{}}],"previous_response_id":"resp_123"}
        with patch.object(gateway,"json_request",return_value={"id":"test"}) as send:
            responses.handle(body)
        self.assertEqual(send.call_args.args,('/v1/responses',body))

    def test_messages_retains_upstream_denial(self):
        with patch.object(gateway,"json_request",side_effect=HTTPException(403,detail="denied")):
            with self.assertRaises(HTTPException) as error:messages.handle({'model':'gpt-5.6','messages':[]})
        self.assertEqual(error.exception.status_code,403)

    def test_models_from_upstream_only(self):
        with patch.object(gateway,"json_request",return_value={'data':[{'id':'real-model'}]}) as send, patch.object(models.model_catalog_service,'list_models',side_effect=AssertionError('legacy catalog')):
            self.assertEqual(models.list_models(),{'data':[{'id':'real-model'}]})
        send.assert_called_once_with('/v1/models')

    def test_sse_function_calls_and_usage(self):
        events=[{'choices':[{'delta':{'tool_calls':[{'id':'call_1'}]},'finish_reason':None}]},{'choices':[{'delta':{},'finish_reason':'tool_calls'}]},{'choices':[],'usage':{'total_tokens':7}}]
        raw=''.join('data: '+json.dumps(e)+'\n\n' for e in events)+'data: [DONE]\n\n'
        r=EventResponse(raw.encode())
        with patch.object(gateway,'_open',return_value=r):self.assertEqual(list(gateway.forward('/v1/chat/completions',{'stream':True})),events)
        self.assertTrue(r.closed)

    def test_response_sse_event_type_preserved(self):
        raw=b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n'
        with patch.object(gateway,'_open',return_value=EventResponse(raw)):
            self.assertEqual(list(gateway.forward('/v1/responses',{'stream':True}))[0]['type'],'response.completed')

    def test_truncated_sse_fails(self):
        with patch.object(gateway,'_open',return_value=EventResponse(b'data: {"choices":[]}\n\n')):
            with self.assertRaises(HTTPException):list(gateway.forward('/v1/chat/completions',{'stream':True}))

    def test_sse_error_never_succeeds(self):
        with patch.object(gateway,'_open',return_value=EventResponse(b'data: {"type":"error","error":{"message":"secret upstream detail"}}\n\n')):
            with self.assertRaises(HTTPException) as error:list(gateway.forward('/v1/responses',{'stream':True}))
        self.assertNotIn('secret',str(error.exception.detail))

    def test_http_errors_redacted(self):
        import urllib.error
        error=urllib.error.HTTPError('http://private',429,'bad',{},io.BytesIO(b'secret'))
        with patch('urllib.request.urlopen',side_effect=error):
            with self.assertRaises(HTTPException) as caught:gateway.forward('/v1/chat/completions',{})
        self.assertEqual(caught.exception.status_code,429);self.assertNotIn('secret',str(caught.exception.detail))

    def test_search_uses_response_tools(self):
        result={'output':[{'type':'message','content':[{'type':'output_text','text':'Answer','annotations':[{'type':'url_citation','url':'https://example.com','title':'Source'}]}]}]}
        with patch.object(gateway,'forward',return_value=result) as send:
            answer=openai_search.handle({'prompt':'query'})
        self.assertEqual(send.call_args.args[0],'/v1/responses');self.assertEqual(answer['sources'][0]['url'],'https://example.com');self.assertEqual(answer['answer'],'Answer')

    def test_web_constructor_blocked(self):
        from services.openai_backend_api import OpenAIBackendAPI
        with self.assertRaises(HTTPException) as error:OpenAIBackendAPI('test')
        self.assertEqual(error.exception.status_code,501)

    def test_scheduler_not_using_local_pool(self):
        from api.ai import _text_scheduler_runner, _image_scheduler_runner
        from services.scheduler.models import Lane
        self.assertIsNone(_text_scheduler_runner('auto'));self.assertIsNone(_image_scheduler_runner('gpt-image-2',{},Lane.IMAGE_GENERATION))

    def test_chat_image_adapter_uses_images_upstream(self):
        from services.protocol.conversation import stream_image_outputs_with_pool,ConversationRequest
        with patch('services.protocol.image_upstream.generate',return_value=iter([])) as send:
            list(stream_image_outputs_with_pool(ConversationRequest(model='gpt-image-2',prompt='test',n=2)))
        self.assertEqual(send.call_args.args[0]['n'],2)

    def test_no_ppt_task_enqueued(self):
        from services.editable_file_task_service import editable_file_task_service
        with self.assertRaises(HTTPException) as error:editable_file_task_service.submit_ppt({'id':'test'},prompt='test')
        self.assertEqual(error.exception.status_code,501)

    def test_account_credentials_delegated(self):
        from services.account_service import account_service
        with patch('services.sub2api_account_info.refresh_credentials',return_value={'access_token':'new'}) as refresh:
            result=account_service._request_access_token_refresh('old-refresh',{'email':'example@example.com'})
        self.assertEqual(result,{'access_token':'new'});refresh.assert_called_once()

    def test_password_login_blocked(self):
        from services.account_service import account_service
        with self.assertRaises(HTTPException):account_service._login_with_password('example@example.com','unused')

    def test_images_bypass_scheduler_without_strict_mode(self):
        from api.ai import _image_scheduler_runner
        from services.scheduler.models import Lane
        with patch.dict(os.environ,{"SUB2API_ONLY":"0","IMAGE_UPSTREAM_CONFIG":"/configured"}), patch('api.ai.scheduler_service.classify_image_generation',side_effect=AssertionError('old pool called')), patch('api.ai.scheduler_service.classify_image_edit',side_effect=AssertionError('old pool called')):
            self.assertIsNone(_image_scheduler_runner('gpt-image-2',{'n':1},Lane.IMAGE_GENERATION))
            self.assertIsNone(_image_scheduler_runner('gpt-image-2',{'n':1},Lane.IMAGE_EDIT))

    def test_background_images_bypass_old_pool(self):
        import asyncio
        from types import SimpleNamespace
        from services.image_task_service import ImageTaskService
        executor=unittest.mock.Mock()
        scheduler=unittest.mock.Mock()
        scheduler.settings.enabled=True
        scheduler.classify_image_generation.side_effect=AssertionError('old pool called')
        obj=SimpleNamespace(scheduler_service=scheduler,_fallback_executor=executor,_run_task=lambda *a:None)
        with patch.dict(os.environ,{"SUB2API_ONLY":"0","IMAGE_UPSTREAM_CONFIG":"/configured"}):
            asyncio.run(ImageTaskService._admit_task(obj,'test','generate',{}, {'id':'test'},'gpt-image-2'))
        executor.submit.assert_called_once()

    def test_responses_images_are_archived(self):
        with patch.object(gateway,'json_request',return_value={'output':[{'type':'image_generation_call','result':'AA=='}]}), patch('services.protocol.image_upstream._archive_items') as save:
            responses.handle({'model':'gpt-5.6','input':'test','tools':[{'type':'image_generation'}]})
        save.assert_called_once_with([{'b64_json':'AA=='}],{})

if __name__=='__main__':unittest.main()
