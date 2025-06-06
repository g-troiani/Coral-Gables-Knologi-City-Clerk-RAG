# Check embeddings
import pandas as pd

entities = pd.read_parquet("graphrag_data/output/entities.parquet")

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