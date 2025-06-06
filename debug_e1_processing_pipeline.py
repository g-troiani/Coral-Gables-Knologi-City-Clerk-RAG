import pandas as pd
import yaml
import re

print("E-1 Processing Pipeline Debug")
print("=" * 50)

# 1. Check if E-1 exists in the ordinance text that was processed
print("\n1. Checking CSV data for ordinance 2024-01...")
df = pd.read_csv('graphrag_data/city_clerk_documents.csv')

# Find ordinance 2024-01 which should contain E-1
ord_2024_01 = df[df['id'].str.contains('2024-01', na=False)]
print(f"Found ordinance 2024-01: {len(ord_2024_01)} entries")

# Check if E-1 is in the text
for idx, row in ord_2024_01.iterrows():
    if 'E-1' in str(row['text']):
        # Find where E-1 appears
        text = row['text']
        matches = list(re.finditer(r'E-1', text, re.IGNORECASE))
        print(f"\nDocument: {row['id']}")
        print(f"E-1 found at {len(matches)} locations")
        for m in matches:
            start = max(0, m.start() - 50)
            end = min(len(text), m.end() + 50)
            print(f"Context: ...{text[start:end]}...")

# 2. Check what text units were created from ordinance 2024-01
print("\n\n2. Checking text units from ordinance 2024-01...")
text_units = pd.read_parquet('graphrag_data/output/text_units.parquet')

# Find units from ordinance 2024-01
ord_units = text_units[text_units['document_ids'].astype(str).str.contains('2024-01', na=False)]
print(f"Text units from ordinance 2024-01: {len(ord_units)}")

# Check if any contain E-1
for idx, unit in ord_units.iterrows():
    if 'E-1' in unit['text']:
        print(f"\nUnit {idx} contains E-1")
        # Show context
        text = unit['text']
        e1_pos = text.find('E-1')
        print(f"Context: ...{text[max(0, e1_pos-100):e1_pos+100]}...")

# 3. Check chunking configuration
print("\n\n3. Checking chunking configuration...")
with open('graphrag_data/settings.yaml', 'r') as f:
    settings = yaml.safe_load(f)
    
print("Chunking settings:")
print(f"Size: {settings['chunks']['size']}")
print(f"Overlap: {settings['chunks']['overlap']}")
print(f"Group by: {settings['chunks']['group_by_columns']}")

print("\n" + "=" * 50)
print("E-1 Processing Pipeline Debug Complete") 