from __future__ import annotations

import requests


HEADERS = {
    "User-Agent": "NJBidRegistryBot/0.2 (+notice-first registry MVP)"
}


def fetch_page(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text
