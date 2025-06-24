# config.py
"""
Environment-driven config for Cosmos Gremlin upload.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT", "")
COSMOS_KEY = os.getenv("COSMOS_KEY", "")
DATABASE = os.getenv("COSMOS_DATABASE", "cgGraph")
CONTAINER = os.getenv("COSMOS_CONTAINER", "cityClerk")

PARTITION_KEY = os.getenv("COSMOS_PARTITION_KEY", "partitionKey")
PARTITION_VALUE = os.getenv("COSMOS_PARTITION_VALUE", "demo")


def validate_config() -> bool:
    missing = [k for k in ("COSMOS_KEY", "COSMOS_ENDPOINT") if not globals()[k]]
    if missing:
        print("❌ Missing env vars:", ", ".join(missing))
        print("Add them to .env:\n  COSMOS_KEY=…\n  COSMOS_ENDPOINT=…")
        return False
    print("✅ Cosmos configuration OK")
    return True


if __name__ == "__main__":
    validate_config() 