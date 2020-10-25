import uuid
from datetime import datetime
from typing import List, Dict
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel


class SessionEventsRepository():

    def __init__(self):
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
    def _typecast(event):
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
        for event in events:
            self.batch.add(
                self.insert_event,
                SessionEventsRepository._typecast(event)
                )
        try:
            self.session.execute(self.batch)
        finally:
            self.batch.clear()

    def fetch_recent_completed_sessions(self, player_id: str):
        rows = self.session.execute("""
            SELECT session_id
            FROM unity_assignment.session_events
            WHERE player_id=%s AND event=%s
            LIMIT 20;
            """,
            (uuid.UUID(player_id),'end'))
        return [str(row.session_id) for row in rows]
