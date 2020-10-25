import bz2
import json
import unittest
from typing import List, Dict
from router_service import RouterService

class SessionEventsServiceIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.router = RouterService()

    @property
    def _insert_events_request(self) -> Dict:
        return {
            'httpMethod': 'POST',
            'path': '/v1/insert',
            'queryStringParameters': '',
            'headers': {},
            'body': json.dumps(self._events_batch),
            'isBase64Encoded': False,
        }

    @property
    def _recent_events_request(self) -> Dict:
        return {
            'httpMethod': 'GET',
            'path': '/v1/recent',
            'queryStringParameters': {'player_id':'cff7a233780b43ca8b256260b6759f0d'},
            'headers': {},
            'body': '',
            'isBase64Encoded': False,
        }

    @property
    def _events_batch(self):
        return [
            {'player_id': 'cff7a233780b43ca8b256260b6759f0d', 'country': 'CA', 'event': 'start', 'session_id': '2ab87ad3-4542-4a8e-b9b9-40c609ffdad5', 'ts': '2016-11-28T20:50:22'},
            {'player_id': 'cff7a233780b43ca8b256260b6759f0d', 'event': 'end', 'session_id': '2ab87ad3-4542-4a8e-b9b9-40c609ffdad5', 'ts': '2016-11-29T04:18:25'},
            {'player_id': '7569b1ec9a934730bfe0891d88c47041', 'country': 'US', 'event': 'start', 'session_id': 'a702cf5c-d837-4017-9b63-2ee836faa76d', 'ts': '2016-11-15T18:48:02'}
            {'player_id': 'cff7a233780b43ca8b256260b6759f0d', 'country': 'CA', 'event': 'start', 'session_id': 'b7f09fe5-bd3b-4370-a576-5ae73e9a7851', 'ts': '2016-11-24T20:50:22'},
            {'player_id': 'cff7a233780b43ca8b256260b6759f0d', 'event': 'end', 'session_id': 'b7f09fe5-bd3b-4370-a576-5ae73e9a7851', 'ts': '2016-11-25T04:18:25'},
            {'player_id': 'cff7a233780b43ca8b256260b6759f0d', 'country': 'CA', 'event': 'start', 'session_id': '70620aea-1bff-4924-9d3a-06cfefb3ac1b', 'ts': '2016-11-29T04:49:19'},
        ]

    def test_smoke_test(self):
        response = self.router.handle(self._insert_events_request)
        self.assertEqual(response['statusCode'], 200, response['body'])
        self.assertEqual(response['statusDescription'], '200 OK')
        self.assertEqual(json.loads(response['body']), 'Successfully added events')

        response = self.router.handle(self._recent_events_request)
        self.assertEqual(response['statusCode'], 200, response['body'])
        self.assertEqual(response['statusDescription'], '200 OK')
        self.assertEqual(json.loads(response['body']), ['2ab87ad3-4542-4a8e-b9b9-40c609ffdad5','b7f09fe5-bd3b-4370-a576-5ae73e9a7851'])

    def tearDown(self):
        self.router.session_events_service.repository.session.execute(
            'DROP KEYSPACE IF EXISTS unity_assignment'
            )
