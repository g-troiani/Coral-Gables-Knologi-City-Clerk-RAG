import pandas as pd
import re

# Check the CSV for E-1
df = pd.read_csv('graphrag_data/city_clerk_documents.csv')

# Find all E-1 mentions
for idx, row in df.iterrows():
    if 'E-1' in str(row['text']):
        print(f"\nDocument: {row['id']}")
        print(f"Title: {row['title']}")
        
        # Find E-1 context
        text = row['text']
        matches = list(re.finditer(r'E-1', text, re.IGNORECASE))
        print(f"E-1 found {len(matches)} times")
        
        for i, match in enumerate(matches[:2]):  # First 2 matches
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)
            print(f"\nMatch {i+1}: ...{text[start:end]}...") 