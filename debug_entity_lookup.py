# Try to query for a known entity
import pandas as pd

entities = pd.read_parquet("graphrag_data/output/entities.parquet")

mayor_entities = entities[entities['type'] == 'PERSON']
if len(mayor_entities) > 0:
    print(f"\nFound {len(mayor_entities)} PERSON entities")
    print("Sample:", mayor_entities['title'].head(5).tolist()) 