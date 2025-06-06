import pandas as pd

# Check if E-1 exists in entities
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