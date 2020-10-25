from typing import List, Dict
from session_events_repository import SessionEventsRepository

class SessionEventsService():

    def __init__(self):
        self.repository = SessionEventsRepository()

    def insert_events_batch(self, events: List[Dict[str,str]]):
        if isinstance(events, list):
            if 1 <= len(events) <= 10:
                self.repository.insert_events_batch(events=events)
                return 'Successfully added events'
            else:
                raise ValueError('Each batch must be of size between 1 and 10')
        else:
            raise TypeError('A list is required for the batch of events')

    def fetch_recent_completed_events(self, params: List[Dict[str,str]]):
        player_id = params.get('player_id')
        if isinstance(player_id, str):
            return self.repository.fetch_recent_completed_events(player_id)
        else:
            raise TypeError('player_id must be of type string')
