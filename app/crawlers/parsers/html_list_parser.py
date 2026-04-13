from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from .base import BaseParser


class HTMLListParser(BaseParser):
    def extract_items(self, html: str, source: Any) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for anchor in soup.select("a[href]")[:25]:
            title = anchor.get_text(" ", strip=True)
            href = anchor.get("href")
            if title and href:
                items.append({"title": title, "url": href})
        return items


HtmlListParser = HTMLListParser
