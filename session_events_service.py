from typing import List, Dict
from session_events_repository import SessionEventsRepository

class SessionEventsService():
    """
    class that interfaces session event repository
    """

    def __init__(self):
        """
        initializes SessionEventsService
        """
        self.repository = SessionEventsRepository()

    def insert_events_batch(self, events: List[Dict[str,str]]) -> str:
        """
        inserts events batch into the SessionEvents database
        :param events: list of dictionary of events such as:
        [{
            "event": "start",
            "country": "FI",
            "player_id": "0a2d12a1a7e145de8bae44c0c6e06629", "session_id": "4a0c43c9-c43a-42ff-ba55-67563dfa35d4", "ts": "2016-12-02T12:48:05.520022"
        },
        {
            "event": "end",
            "player_id": "0a2d12a1a7e145de8bae44c0c6e06629", "session_id": "4a0c43c9-c43a-42ff-ba55-67563dfa35d4", "ts": "2016-12-02T12:49:05.520022"
        }]
        :return: http body message
        """
        if isinstance(events, list):
            if 1 <= len(events) <= 10:
                self.repository.insert_events_batch(events=events)
                return 'Successfully added events'
            else:
                raise ValueError('Each batch must be of size between 1 and 10')
        else:
            raise TypeError('A list is required for the batch of events')

    def fetch_recent_completed_sessions(self, params: Dict[str,str]) -> List[str]:
        """
        fetches the recent completed sessions of a user from the SessionEvents database
        :param params: dictionary holding player_id
        :return: list of recent session_ids
        """
        player_id = params.get('player_id')
        if isinstance(player_id, str):
            return self.repository.fetch_recent_completed_sessions(player_id)
        else:
            raise TypeError('player_id must be of type string')
