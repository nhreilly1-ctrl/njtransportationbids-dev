from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseParser(ABC):
    @abstractmethod
    def extract_items(self, html: str, source: Any) -> list[dict[str, Any]]:
        raise NotImplementedError
