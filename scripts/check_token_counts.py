#!/usr/bin/env python3
"""Check token counts in the GraphRAG CSV file."""

import pandas as pd
import tiktoken

def main():
    df = pd.read_csv('graphrag_data/city_clerk_documents.csv')
    encoding = tiktoken.get_encoding("cl100k_base")
    
    print("📊 Token count analysis:")
    print("=" * 50)
    
    total_tokens = 0
    long_docs = []
    
    for idx, row in df.iterrows():
        tokens = len(encoding.encode(str(row['text'])))
        total_tokens += tokens
        
        print(f"{row['id']}: {tokens} tokens")
        
        if tokens > 8000:
            long_docs.append((row['id'], tokens))
            print(f"  ⚠️  WARNING: Document may be too long!")
        elif tokens > 4000:
            print(f"  📝 Long document")
    
    print("\n" + "=" * 50)
    print(f"📈 Summary:")
    print(f"   Total documents: {len(df)}")
    print(f"   Total tokens: {total_tokens:,}")
    print(f"   Average tokens per document: {total_tokens // len(df):,}")
    print(f"   Documents > 8000 tokens: {len(long_docs)}")
    
    if long_docs:
        print(f"\n⚠️  Long documents that may cause issues:")
        for doc_id, tokens in long_docs:
            print(f"   - {doc_id}: {tokens:,} tokens")
        print(f"\nConsider splitting these documents or truncating them.")

if __name__ == "__main__":
    main() 