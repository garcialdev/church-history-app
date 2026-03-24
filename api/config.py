# import os

# DATABASE_URL = os.getenv(
#     "DATABASE_URL",
#     "postgresql://miguel:YOUR_PASSWORD@localhost:5432/YOUR_DB_NAME"
# )

# NOCODB_BASE_URL = os.getenv("NOCODB_BASE_URL", "https://nocodb.bbs1.net")

# WIKIPEDIA_API_URL = "https://en.wikipedia.org/api/rest_v1/page/summary"

import os
import secrets

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://miguel:YOUR_PASSWORD@localhost:5432/YOUR_DB_NAME"
)

NOCODB_BASE_URL = os.getenv("NOCODB_BASE_URL", "https://nocodb.bbs1.net")
WIKIPEDIA_API_URL = "https://en.wikipedia.org/api/rest_v1/page/summary"

# Admin password — set ADMIN_PASSWORD in your .env file
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")

# Simple in-memory token store (sufficient for single-user personal app)
# Maps token → True. Cleared on API restart.
_valid_tokens: set = set()

def create_token() -> str:
    token = secrets.token_hex(32)
    _valid_tokens.add(token)
    return token

def validate_token(token: str) -> bool:
    return token in _valid_tokens

def revoke_token(token: str):
    _valid_tokens.discard(token)