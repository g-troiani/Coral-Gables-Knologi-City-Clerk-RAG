# Check text units for E-1 mentions
import pandas as pd

text_units = pd.read_parquet("graphrag_data/output/text_units.parquet")
e1_texts = text_units[text_units['text'].str.contains('E-1', case=False, na=False)]
print(f"\nText chunks mentioning E-1: {len(e1_texts)}") 