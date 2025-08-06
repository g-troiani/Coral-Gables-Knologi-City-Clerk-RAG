#!/usr/bin/env python3
"""Test vector database connection and credentials."""

import os
import sys
from azure.search.documents.indexes import SearchIndexClient
from azure.core.credentials import AzureKeyCredential
from openai import AzureOpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def test_connection():
    """Test Azure Cognitive Search and OpenAI connections."""
    print("🔍 Testing Vector Database Connection...\n")
    
    # Check environment variables
    search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT", "").strip()
    search_key = os.getenv("VECTOR_DATABASE_KEY", "").strip()
    index_name = os.getenv("VECTOR_DATABASE_NAME", "city-clerk-rag").strip()
    
    print("1️⃣ Checking environment variables:")
    print(f"   AZURE_SEARCH_ENDPOINT: {'✅ Set' if search_endpoint else '❌ Not set'}")
    print(f"   VECTOR_DATABASE_KEY: {'✅ Set' if search_key else '❌ Not set'}")
    print(f"   VECTOR_DATABASE_NAME: {index_name}")
    
    if not search_endpoint or not search_key:
        print("\n❌ Missing required environment variables!")
        print("   Please set AZURE_SEARCH_ENDPOINT and VECTOR_DATABASE_KEY")
        return False
    
    # Test Azure Cognitive Search connection
    print("\n2️⃣ Testing Azure Cognitive Search connection...")
    try:
        client = SearchIndexClient(
            endpoint=search_endpoint,
            credential=AzureKeyCredential(search_key)
        )
        # Try to list indices
        indices = list(client.list_indexes())
        print(f"   ✅ Connected successfully! Found {len(indices)} indices")
        
        # Check if our index exists
        index_names = [idx.name for idx in indices]
        if index_name in index_names:
            print(f"   ✅ Index '{index_name}' exists")
        else:
            print(f"   ⚠️  Index '{index_name}' does not exist (will be created on first run)")
            
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        return False
    
    # Test OpenAI embeddings
    print("\n3️⃣ Testing Azure OpenAI embeddings...")
    embeddings_deployment = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT", "").strip()
    
    if not embeddings_deployment:
        print("   ⚠️  AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT not set")
        print("   Using default: text-embedding-ada-002")
        embeddings_deployment = "text-embedding-ada-002"
    
    try:
        openai_client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        # Test embedding generation
        response = openai_client.embeddings.create(
            model=embeddings_deployment,
            input="Test"
        )
        
        print(f"   ✅ Embeddings working! Deployment: {embeddings_deployment}")
        print(f"   ✅ Embedding dimension: {len(response.data[0].embedding)}")
        
    except Exception as e:
        print(f"   ❌ Embeddings failed: {e}")
        print(f"   Make sure your deployment name '{embeddings_deployment}' is correct")
        return False
    
    print("\n✅ All vector database components are properly configured!")
    return True


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)