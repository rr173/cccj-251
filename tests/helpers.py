"""Shared helpers for the test suite."""

from __future__ import annotations

import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for path in (ROOT, HERE):
    if path not in sys.path:
        sys.path.insert(0, path)

from inspection_store import DeviceStore  # noqa: E402


class TempStore:
    def __init__(self, name: str = "store.db") -> None:
        self.directory = tempfile.mkdtemp(prefix="insp-")
        self.path = os.path.join(self.directory, name)

    def create(self) -> DeviceStore:
        return DeviceStore(self.path, create=True)

    def open(self) -> DeviceStore:
        return DeviceStore(self.path)
