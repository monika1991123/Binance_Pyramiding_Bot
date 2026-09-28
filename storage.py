import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any


def atomic_json_write(path: Path, payload: Any, mode: int = 0o600) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, path)
        try:
            os.chmod(path, mode)
        except OSError:
            pass
    finally:
        if os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def read_json(path: Path, default: Any) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError):
        return default


class JsonState:
    def __init__(self, path: Path, default_factory):
        self.path = Path(path)
        self.default_factory = default_factory
        self.lock = threading.RLock()
        self.data = read_json(self.path, None)
        if not isinstance(self.data, dict):
            self.data = default_factory()
            self.save()

    def snapshot(self) -> dict:
        with self.lock:
            return json.loads(json.dumps(self.data))

    def save(self) -> None:
        with self.lock:
            atomic_json_write(self.path, self.data)

    def mutate(self, callback) -> Any:
        with self.lock:
            result = callback(self.data)
            self.save()
            return result
