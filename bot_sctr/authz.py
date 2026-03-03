from typing import Dict, Optional

class Authz:
    def __init__(self):
        self.users: Dict[int, Dict] = {}

    def load(self, records: list[dict]):
        self.users = {}
        for r in records:
            try:
                uid = int(str(r.get("user_id", "")).strip())
            except:
                continue
            self.users[uid] = r

    def get_user(self, user_id: int) -> Optional[Dict]:
        return self.users.get(user_id)

    def is_allowed(self, user_id: int) -> bool:
        u = self.get_user(user_id)
        if not u:
            return False
        return str(u.get("activo", "")).strip() == "1"

    def role(self, user_id: int) -> str:
        u = self.get_user(user_id)
        if not u:
            return "no_autorizado"
        return str(u.get("rol", "tecnico")).strip() or "tecnico"