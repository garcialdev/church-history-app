import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://miguel:YOUR_PASSWORD@localhost:5432/YOUR_DB_NAME"
)

NOCODB_BASE_URL = os.getenv("NOCODB_BASE_URL", "https://nocodb.bbs1.net")

WIKIPEDIA_API_URL = "https://en.wikipedia.org/api/rest_v1/page/summary"