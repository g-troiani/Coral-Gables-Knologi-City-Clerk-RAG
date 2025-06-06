import pandas as pd

# Check the CSV that GraphRAG is reading
df = pd.read_csv('graphrag_data/city_clerk_documents.csv')

# Find ordinance 2024-01 which should contain E-1
ord_2024_01 = df[df['id'].str.contains('2024-01', na=False)]
print(f"Found ordinance 2024-01: {len(ord_2024_01)} entries")

# Check if E-1 is in the text
for idx, row in ord_2024_01.iterrows():
    if 'E-1' in str(row['text']):
        # Find where E-1 appears
        text = row['text']
        import re
        matches = list(re.finditer(r'E-1', text, re.IGNORECASE))
        print(f"\nDocument: {row['id']}")
        print(f"E-1 found at {len(matches)} locations")
        for m in matches:
            start = max(0, m.start() - 50)
            end = min(len(text), m.end() + 50)
            print(f"Context: ...{text[start:end]}...") 