from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from .base import BaseParser


class LegalNoticeParser(BaseParser):
    def extract_items(self, html: str, source: Any) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for node in soup.select("a[href], article, li")[:25]:
            text = node.get_text(" ", strip=True)
            href = node.get("href") if getattr(node, "get", None) else None
            if text:
                items.append({"title": text[:300], "url": href or source.effective_notice_entry_url})
        return items
