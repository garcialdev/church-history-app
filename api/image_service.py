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
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                thumbnail = data.get("thumbnail", {})
                return thumbnail.get("source")
    except Exception:
        pass
    return None


async def resolve_image(thumbnail_json: Optional[str], name: Optional[str]) -> Optional[str]:
    """
    Priority: NocoDB thumbnail → Wikipedia fallback → None
    """
    nocodb_url = parse_nocodb_thumbnail(thumbnail_json)
    if nocodb_url:
        return nocodb_url

    if name:
        wiki_url = await fetch_wikipedia_image(name)
        if wiki_url:
            return wiki_url

    return None