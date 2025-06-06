import pandas as pd

print("GraphRAG Debug Comprehensive Check")
print("=" * 50)

# 1. Check if E-1 exists in entities
entities = pd.read_parquet("graphrag_data/output/entities.parquet")

# Look for E-1 specifically
e1_entities = entities[entities['title'].str.contains('E-1', case=False, na=False)]
print(f"E-1 entities found: {len(e1_entities)}")
if len(e1_entities) > 0:
    print(e1_entities[['id', 'title', 'type', 'description']].head())

# Show all agenda items
agenda_items = entities[entities['type'] == 'AGENDA_ITEM']
print(f"\nAll agenda items ({len(agenda_items)}):")
print(agenda_items[['title', 'description']].head(10))

# 2. Check embeddings
print("\nChecking embeddings:")
if 'embedding' in entities.columns:
    print("Entity embeddings exist:", entities['embedding'].notna().sum())
else:
    print("WARNING: No embedding column found!")

# Check text unit embeddings
text_units = pd.read_parquet("graphrag_data/output/text_units.parquet")
if 'embedding' in text_units.columns:
    print("Text unit embeddings exist:", text_units['embedding'].notna().sum())
else:
    print("WARNING: No text unit embedding column found!")

# 3. Check community reports - these should have content
reports = pd.read_parquet("graphrag_data/output/community_reports.parquet")
print(f"\nCommunity reports: {len(reports)}")
if len(reports) > 0:
    print("Sample report:", reports.iloc[0]['summary'][:200] if 'summary' in reports.columns else "No summary column")

# 4. Test if GraphRAG query is working at all with a direct entity lookup
mayor_entities = entities[entities['type'] == 'PERSON']
if len(mayor_entities) > 0:
    print(f"\nFound {len(mayor_entities)} PERSON entities")
    print("Sample:", mayor_entities['title'].head(5).tolist())

print("\n" + "=" * 50)
print("Debug check complete") 