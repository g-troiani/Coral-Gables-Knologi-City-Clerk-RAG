# Check text units
import pandas as pd

text_units = pd.read_parquet('graphrag_data/output/text_units.parquet')

# Find units from ordinance 2024-01
ord_units = text_units[text_units['document_ids'].astype(str).str.contains('2024-01', na=False)]
print(f"\nText units from ordinance 2024-01: {len(ord_units)}")

# Check if any contain E-1
for idx, unit in ord_units.iterrows():
    if 'E-1' in unit['text']:
        print(f"\nUnit {idx} contains E-1")
        # Show context
        text = unit['text']
        e1_pos = text.find('E-1')
        print(f"Context: ...{text[max(0, e1_pos-100):e1_pos+100]}...") 