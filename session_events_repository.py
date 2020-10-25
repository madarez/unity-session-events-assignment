import uuid
from datetime import datetime
from typing import List, Dict
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel


class SessionEventsRepository():
    """
    class to interact with SessionEvents database
    """

    def __init__(self):
        """
        Sets up the cassandra cluster, session, keyspace, table, and a reusable
        batch statement
        in order for the data older than 1 year old to be discarded, we set a
        default ttl of 365*24*60*60 or 31536000 seconds.
        There are multiple configurations including the default ttl that may
        be set in a separate configuration file.
        """
        # connect to cluster
        cluster = Cluster()
        self.session = cluster.connect()
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS unity_assignment
            WITH REPLICATION = { 
                'class' : 'SimpleStrategy',
                'replication_factor' : 1
            };
            """)
        # TODO: POC if separating start and end event would be more efficient
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS unity_assignment.session_events (
                event TEXT,
                country TEXT,
                player_id UUID,
                session_id UUID,
                ts timestamp,
                PRIMARY KEY (player_id, event, ts)
            )  WITH default_time_to_live = 31536000 AND CLUSTERING ORDER BY (event DESC, ts DESC); 
            """)

        self.batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        self.insert_event = self.session.prepare(
            """INSERT INTO unity_assignment.session_events
            (event, country, player_id, session_id, ts)
            VALUES (?, ?, ?, ?, ?)
            """)

    @staticmethod
    def _typecast(event: Dict):
        """
        casts type of passed events to be stored into Session Events table
        :param event: event whose fields' types are going to be cast
        """
        if isinstance(event, dict):
            return (
                event.get('event'),
                event.get('country'),
                uuid.UUID(event.get('player_id')),
                uuid.UUID(event.get('session_id')),
                datetime.fromisoformat(event.get('ts'))
                )
        else:
            raise TypeError(f'{type(event)} is an unsupported type for the '
                             'passed event to be typecast')

    def insert_events_batch(self, events: List[Dict[str,str]]):
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
        for event in events:
            self.batch.add(
                self.insert_event,
                SessionEventsRepository._typecast(event)
                )
        try:
            self.session.execute(self.batch)
        finally:
            self.batch.clear()

    def fetch_recent_completed_sessions(self, player_id: str) -> List[str]:
        """
        fetches up to 20 recent completed sessions associated with a player
        :param player_id: the player id of the player whose recent sessions
        we'd want to query
        :returns: list of recent sessions sorted from latest to the earliest
        """
        rows = self.session.execute("""
            SELECT session_id
            FROM unity_assignment.session_events
            WHERE player_id=%s AND event=%s
            LIMIT 20;
            """,
            (uuid.UUID(player_id),'end'))
        # TODO: We may want to query the same rows from start events to make
        # sure the events correspond to begin events as well. Alternatively,
        # we may redisign table, say to normalize the pair of start and end
        # events in the same row, to order the results by the timestamps, or
        # again work out at application level. However, this might come for
        # free from the downstream. Therefore, I just assume the start events
        # are guaranteed to exist for end events.
        return [str(row.session_id) for row in rows]
