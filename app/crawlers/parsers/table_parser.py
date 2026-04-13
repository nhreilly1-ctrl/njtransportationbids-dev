from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from .base import BaseParser


class TableParser(BaseParser):
    def extract_items(self, html: str, source: Any) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for row in soup.select("table tr")[:25]:
            text = " ".join(cell.get_text(" ", strip=True) for cell in row.select("th, td")).strip()
            link = row.select_one("a[href]")
            if text:
                items.append({"title": text[:300], "url": link.get("href") if link else source.effective_notice_entry_url})
        return items
