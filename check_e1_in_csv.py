import pandas as pd
import re

df = pd.read_csv('graphrag_data/city_clerk_documents.csv')
e1_docs = df[df['text'].str.contains('E-1', case=False, na=False)]
print(f'Documents with E-1: {len(e1_docs)}')
for _, row in e1_docs.iterrows():
    print(f"\n{row['id']}:")
    text = row['text']
    for m in re.finditer(r'E-1', text, re.I):
        s = max(0, m.start()-50)
        e = min(len(text), m.end()+50)
        print(f'  ...{text[s:e]}...') 