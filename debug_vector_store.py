import pandas as pd

# Load entities to see what we have
entities = pd.read_parquet('graphrag_data/output/entities.parquet')

# Check for mayor-related entities
mayor_entities = entities[entities['title'].str.contains('MAYOR', case=False, na=False)]
print(f"Mayor entities: {len(mayor_entities)}")
print(mayor_entities[['title', 'type', 'description']].head())

# Check for E-1
e1_entities = entities[entities['title'].str.contains('E-1', case=False, na=False) | 
                       entities['description'].str.contains('E-1', case=False, na=False)]
print(f"\nE-1 related entities: {len(e1_entities)}")
if len(e1_entities) > 0:
    print(e1_entities[['title', 'type', 'description']].head()) 