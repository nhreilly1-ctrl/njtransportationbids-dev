from __future__ import annotations

from typing import Any

from .base import BaseParser


class FallbackParser(BaseParser):
    def extract_items(self, html: str, source: Any) -> list[dict[str, Any]]:
        return []
