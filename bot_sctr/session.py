from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

@dataclass
class Session:
    state: str = "IDLE"
    ctx: Dict[str, Any] = field(default_factory=dict)
    last_activity: datetime = field(default_factory=datetime.utcnow)

class SessionManager:
    def __init__(self, ttl_minutes: int):
        self.ttl = timedelta(minutes=ttl_minutes)
        self._sessions: Dict[int, Session] = {}

    def get(self, user_id: int) -> Session:
        s = self._sessions.get(user_id)
        if not s:
            s = Session()
            self._sessions[user_id] = s
        return s

    def touch(self, user_id: int):
        self.get(user_id).last_activity = datetime.utcnow()

    def reset(self, user_id: int):
        self._sessions[user_id] = Session()

    def is_expired(self, user_id: int) -> bool:
        s = self.get(user_id)
        return datetime.utcnow() - s.last_activity > self.ttl