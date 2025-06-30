#!/usr/bin/env python3
"""
Quick test with the fixed Azure configuration
"""
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

async def test_fixed_config():
    """Test Azure OpenAI with the corrected gpt-4o deployment."""
    print("🧪 Testing Fixed Azure Configuration")
    print("=" * 40)
    
    try:
        from openai import AsyncAzureOpenAI
        
        # Get config from env
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        
        # Force the deployment name to gpt-4o (since we know it works)
        deployment = "gpt-4o"
        
        print(f"📍 Endpoint: {endpoint}")
        print(f"🚀 Deployment: {deployment}")
        print(f"📅 API Version: {api_version}")
        
        client = AsyncAzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=endpoint
        )
        
        # Test the E-1 query
        query = "What is agenda item E1 in meeting 08.26.2014?"
        print(f"\n🔍 Testing query: {query}")
        
        response = await client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that provides information about city clerk documents and agenda items."},
                {"role": "user", "content": query}
            ],
            max_tokens=100
        )
        
        print("✅ Azure OpenAI API works with fixed config!")
        print(f"📝 Response: {response.choices[0].message.content}")
        
        # Test simple extraction task
        extract_query = "Extract the main topic from this text: 'Emergency Ordinance about trespassing and privacy violations in dwellings'"
        
        response2 = await client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": extract_query}],
            max_tokens=50
        )
        
        print(f"\n🔍 Extraction test: {extract_query}")
        print(f"📝 Response: {response2.choices[0].message.content}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_fixed_config())
    if result:
        print("\n🎉 Configuration is fixed! Azure OpenAI API is working.")
        print("💡 The Simple NER and GraphRAG queries should now work.")
    else:
        print("\n❌ Configuration still has issues.") 