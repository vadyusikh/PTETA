from datetime import datetime
from threading import Lock


class LockingCounter:
    def __init__(self):
        self.lock = Lock()
        self.count = 0
        self.is_shown = False
        self.last_update = datetime.now()

    def increment(self, offset=1) -> None:
        with self.lock:
            self.count += offset

    def get_count(self) -> int:
        with self.lock:
            return self.count

    def set_count(self, value=0) -> None:
        with self.lock:
            self.count = value

    def get_is_shown(self) -> bool:
        with self.lock:
            return self.is_shown

    def set_is_shown(self, value: bool) -> None:
        with self.lock:
            self.is_shown = value

    def get_last_update(self) -> datetime:
        with self.lock:
            return self.last_update

    def set_last_update(self, value: datetime) -> None:
        with self.lock:
            self.last_update = value
