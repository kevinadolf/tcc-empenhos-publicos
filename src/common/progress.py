"""Thread-safe progress tracking helpers."""

from __future__ import annotations

from threading import Lock
from typing import Any, Callable, Dict, List, Optional

ProgressListener = Callable[[Dict[str, Any]], None]


class ProgressTracker:
    """Tracks progress (0-100) and notifies listeners on meaningful changes."""

    def __init__(self, name: str = "task") -> None:
        self._lock = Lock()
        self._state: Dict[str, Any] = {
            "name": name,
            "status": "idle",
            "progress": 0.0,
            "message": "Idle",
        }
        self._listeners: List[ProgressListener] = []
        self._last_notified_progress: float = -1.0
        self._last_notified_message: Optional[str] = None

    def start(self, message: str) -> None:
        with self._lock:
            self._state.update({"status": "running", "progress": 0.0, "message": message})
            self._last_notified_progress = -1.0
            self._last_notified_message = None
            self._notify_locked()

    def update(self, progress: float, message: Optional[str] = None) -> None:
        progress = max(0.0, min(100.0, float(progress)))
        with self._lock:
            if self._state["status"] == "idle":
                self._state["status"] = "running"
            changed = abs(progress - self._state["progress"]) >= 0.5
            self._state["progress"] = progress
            if message is not None and message != self._state["message"]:
                self._state["message"] = message
                changed = True
            if changed:
                self._notify_locked()

    def complete(self, message: str = "Completed") -> None:
        with self._lock:
            self._state.update({"status": "completed", "progress": 100.0, "message": message})
            self._notify_locked(force=True)

    def fail(self, message: str) -> None:
        with self._lock:
            self._state.update({"status": "failed", "message": message})
            self._notify_locked(force=True)

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def subscribe(self, listener: ProgressListener) -> None:
        with self._lock:
            self._listeners.append(listener)

    def _notify_locked(self, force: bool = False) -> None:
        progress = float(self._state["progress"])
        message = str(self._state["message"])
        if not force:
            if (
                abs(progress - self._last_notified_progress) < 1.0
                and message == self._last_notified_message
            ):
                return
        self._last_notified_progress = progress
        self._last_notified_message = message
        snapshot = dict(self._state)
        for listener in list(self._listeners):
            try:
                listener(dict(snapshot))
            except Exception:
                continue
