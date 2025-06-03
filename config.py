#!/usr/bin/env python3
"""
Configuration file for graph database visualization
Manages database credentials and connection settings
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Azure Cosmos DB Gremlin Configuration
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT", "wss://aida-graph-db.gremlin.cosmos.azure.com:443")
COSMOS_KEY = os.getenv("COSMOS_KEY", "")  # This will be set from .env file
DATABASE = os.getenv("COSMOS_DATABASE", "cgGraph")
CONTAINER = os.getenv("COSMOS_CONTAINER", "cityClerk")
PARTITION_KEY = os.getenv("COSMOS_PARTITION_KEY", "partitionKey")
PARTITION_VALUE = os.getenv("COSMOS_PARTITION_VALUE", "demo")

def validate_config():
    """Validate that all required configuration is available"""
    required_vars = {
        "COSMOS_KEY": COSMOS_KEY,
        "COSMOS_ENDPOINT": COSMOS_ENDPOINT,
        "DATABASE": DATABASE,
        "CONTAINER": CONTAINER
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        print("❌ Missing required configuration:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n🔧 Please create a .env file with your credentials:")
        print("   COSMOS_KEY=your_actual_cosmos_key_here")
        print("   COSMOS_ENDPOINT=wss://aida-graph-db.gremlin.cosmos.azure.com:443")
        print("   COSMOS_DATABASE=cgGraph")
        print("   COSMOS_CONTAINER=cityClerk")
        return False
    
    return True

if __name__ == "__main__":
    print("🔧 Configuration Check:")
    if validate_config():
        print("✅ All configuration variables are set!")
    else:
        print("❌ Configuration incomplete!") 