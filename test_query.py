#!/usr/bin/env python3
"""Test query to see exact data format from Cosmos DB."""
import os
import json
from dotenv import load_dotenv
from gremlin_python.driver import client, serializer

load_dotenv()

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE = os.getenv("COSMOS_DATABASE", "cgGraph")
CONTAINER = os.getenv("COSMOS_CONTAINER", "cityClerk")

print("Testing Cosmos DB queries...")

gremlin_client = client.Client(
    f"{COSMOS_ENDPOINT}/gremlin",
    "g",
    username=f"/dbs/{DATABASE}/colls/{CONTAINER}",
    password=COSMOS_KEY,
    message_serializer=serializer.GraphSONSerializersV2d0()
)

try:
    # Test 1: Get a sample vertex with valueMap(true)
    print("\n1. Sample vertex with valueMap(true):")
    print("-" * 50)
    result = gremlin_client.submit("g.V().limit(1).valueMap(true)").all().result()
    if result:
        print(json.dumps(result[0], indent=2, default=str))
    
    # Test 2: Get a Meeting vertex
    print("\n2. Sample Meeting vertex:")
    print("-" * 50)
    result = gremlin_client.submit("g.V().hasLabel('Meeting').limit(1).valueMap(true)").all().result()
    if result:
        print(json.dumps(result[0], indent=2, default=str))
    
    # Test 3: Check edge format
    print("\n3. Sample edge with project:")
    print("-" * 50)
    result = gremlin_client.submit("""g.E().limit(1).project('id','source','target','label')
                                      .by(id())
                                      .by(outV().id())
                                      .by(inV().id())
                                      .by(label())""").all().result()
    if result:
        print(json.dumps(result[0], indent=2, default=str))
    
finally:
    gremlin_client.close() 