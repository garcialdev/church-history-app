import json
import httpx
from typing import Optional
from config import NOCODB_BASE_URL, WIKIPEDIA_API_URL


def parse_nocodb_thumbnail(thumbnail_json: Optional[str]) -> Optional[str]:
    """Extract image URL from NocoDB's JSON thumbnail field."""
    if not thumbnail_json:
        return None
    try:
        data = json.loads(thumbnail_json)
        if isinstance(data, list) and len(data) > 0:
            path = data[0].get("path")
            if path:
                return f"{NOCODB_BASE_URL}/{path}"
    except (json.JSONDecodeError, KeyError, TypeError):
        pass
    return None


async def fetch_wikipedia_image(name: str) -> Optional[str]:
    """Fetch portrait image from Wikipedia REST API."""
    if not name:
        return None
    try:
        search_name = name.strip().replace(" ", "_")
        url = f"{WIKIPEDIA_API_URL}/{search_name}"
        headers = {
            "User-Agent": "ChurchHistoryArchive/1.0 (https://churcharchive.bbs1.net; contact@bbs1.net)"
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                thumbnail = data.get("thumbnail", {})
                return thumbnail.get("source")
    except Exception:
        pass
    return None


async def resolve_image(
    thumbnail_json: Optional[str],
    name: Optional[str],
    wikipedia_name: Optional[str] = None,
    cached_image_url: Optional[str] = None,
) -> Optional[str]:
    """
    Priority:
    1. Cached image URL from DB (manually uploaded or set — always wins)
    2. NocoDB uploaded thumbnail (legacy fallback)
    3. Wikipedia live lookup (only if no cache)
    """
    # 1. Cached URL from DB — manually set/uploaded images always take priority
    if cached_image_url and cached_image_url.strip():
        return cached_image_url.strip()

    # 2. NocoDB uploaded image (legacy — may be broken after server migration)
    nocodb_url = parse_nocodb_thumbnail(thumbnail_json)
    if nocodb_url:
        return nocodb_url

    # 3. Live Wikipedia lookup (slow path — only runs if not cached)
    if wikipedia_name and wikipedia_name.strip():
        wiki_url = await fetch_wikipedia_image(wikipedia_name.strip())
        if wiki_url:
            return wiki_url

    if name:
        wiki_url = await fetch_wikipedia_image(name)
        if wiki_url:
            return wiki_url

    return None