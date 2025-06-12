#!/usr/bin/env python3
"""Analyze what documents were indexed by GraphRAG"""

import pandas as pd

def analyze_documents():
    print("📊 Analyzing GraphRAG Indexed Documents")
    print("=" * 50)
    
    # Read the CSV
    df = pd.read_csv('city_clerk_documents.csv')
    
    print(f"📈 Total Documents Indexed: {len(df)}")
    print(f"📁 CSV Columns: {list(df.columns)}")
    
    print("\n🏷️ Document Types:")
    doc_types = df['document_type'].value_counts()
    for doc_type, count in doc_types.items():
        print(f"   📄 {doc_type}: {count} documents")
    
    print("\n📅 Meeting Dates:")
    dates = df['meeting_date'].value_counts().head(10)
    for date, count in dates.items():
        print(f"   📅 {date}: {count} items")
    
    print("\n🔍 Sample Document IDs:")
    sample_ids = df['id'].head(10).tolist()
    for doc_id in sample_ids:
        print(f"   📋 {doc_id}")
    
    print("\n🎯 Item Code Distribution:")
    item_codes = df['item_code'].value_counts().head(15)
    for code, count in item_codes.items():
        print(f"   🔖 {code}: {count} documents")
    
    # Look for specific document types in the titles
    print("\n📝 Document Type Analysis from Titles:")
    titles = df['title'].fillna('').str.lower()
    
    ordinance_count = titles.str.contains('ordinance').sum()
    resolution_count = titles.str.contains('resolution').sum()
    agenda_count = titles.str.contains('agenda').sum()
    discussion_count = titles.str.contains('discussion').sum()
    update_count = titles.str.contains('update').sum()
    presentation_count = titles.str.contains('presentation').sum()
    
    print(f"   📜 Ordinances: {ordinance_count}")
    print(f"   📋 Resolutions: {resolution_count}")
    print(f"   📑 Agenda Items: {agenda_count}")
    print(f"   💬 Discussions: {discussion_count}")
    print(f"   📈 Updates: {update_count}")
    print(f"   🎤 Presentations: {presentation_count}")

if __name__ == "__main__":
    analyze_documents() 