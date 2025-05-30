#!/usr/bin/env python3
"""Clear Gremlin database only."""
import os
from gremlin_python.driver import client, serializer
from dotenv import load_dotenv

load_dotenv()

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE = os.getenv("COSMOS_DATABASE", "cgGraph")
CONTAINER = os.getenv("COSMOS_CONTAINER", "cityClerk")

print("🗑️  Clearing Gremlin Database...")

try:
    gremlin_client = client.Client(
        f"{COSMOS_ENDPOINT}/gremlin",
        "g",
        username=f"/dbs/{DATABASE}/colls/{CONTAINER}",
        password=COSMOS_KEY,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )
    
    print("📊 Connected to Cosmos DB...")
    
    # Clear graph
    print("Clearing entire graph database...")
    gremlin_client.submit("g.E().drop()").all()
    gremlin_client.submit("g.V().drop()").all()
    
    print("✅ Gremlin database cleared successfully!")
    
    gremlin_client.close()
    
except Exception as e:
    print(f"❌ Error: {e}") 