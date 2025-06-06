import pandas as pd

# Check entities
entities = pd.read_parquet("graphrag_data/output/entities.parquet")

# Look for agenda items
agenda_items = entities[entities['type'].str.contains('agenda', case=False, na=False)]
print(f"Agenda items found: {len(agenda_items)}")
print(agenda_items[['title', 'type']].head(10))

# Also check for E-1 specifically
e1_items = entities[entities['title'].str.contains('E-1', case=False, na=False)]
print(f"\nE-1 mentions: {len(e1_items)}")
print(e1_items[['title', 'type', 'description']].head())

# Check all entity types
print("\nAll entity types found:")
print(entities['type'].value_counts()) 