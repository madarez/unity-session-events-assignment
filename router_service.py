import json
from typing import List, Dict
from session_events_service import SessionEventsService

class RouterService():
    """
    class with main handler routing api requests and prepares http response
    """

    # general response header
    headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
        }

    def __init__(self):
        """
        initializes RouterService
        """
        self.session_events_service = SessionEventsService()

    def handle(self, request: Dict) -> Dict:
        """
        handles the http requests and responds with appropriate status code
        :param request: request dictionary including path, httpMethod, body,
        and queryStringParameters
        :return: http response
        """
        path: str = request['path']
        method: str = request['httpMethod']
        try:
            if method == 'GET':
                params: Dict[str, Any] = request.get('queryStringParameters')
            elif method == 'POST':
                params = json.loads(request['body'])
            else:
                # if a wrong method is passed, we need to return the allowed
                # methods in the header's Allow as follows
                headers = self.headers.copy()
                headers['Allow']='GET, POST'
                return {
                        'statusCode': 405,
                        'statusDescription': '405 Method Not Allowed',
                        'isBase64Encoded': False,
                        'headers': headers,
                        'body': ''
                    }
            # receiving event batches (1-10 events / batch)
            if path == '/v1/insert':
                result = self.session_events_service.insert_events_batch(params)
            # fetch last 20 complete sessions for a given player
            elif path == '/v1/recent':
                result = self.session_events_service.fetch_recent_completed_sessions(params)
            else:
                return {
                        'statusCode': 404,
                        'statusDescription': '404 Not Found',
                        'isBase64Encoded': False,
                        'headers': self.headers,
                        'body': 'Resource not found'
                    }
        except Exception as e:
            return {
                        'statusCode': 400,
                        'statusDescription': '400 Bad Request',
                        'isBase64Encoded': False,
                        'headers': self.headers,
                        'body': str(e)
                    }
        else:
            return {
                    'statusCode': 200,
                    'statusDescription': '200 OK',
                    'isBase64Encoded': False,
                    'headers': self.headers,
                    'body': json.dumps(result)
                }
