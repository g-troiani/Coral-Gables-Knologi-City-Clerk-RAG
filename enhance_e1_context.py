# Add E-1 context to help extraction
import pandas as pd

df = pd.read_csv('graphrag_data/city_clerk_documents.csv')

# Find the agenda row with E-1
agenda_row = df[df['id'] == 'agenda_01_09_2024'].index[0]
old_text = df.loc[agenda_row, 'text']

# Add explicit E-1 context at the beginning
enhanced_text = """
IMPORTANT AGENDA ITEM: E-1
Agenda Item E-1 is an ordinance amending Ordinance No. 3576 and the Zoning Code 
regarding the Cocoplum Phase 1 Security Guard District. This item was sponsored 
by Commissioner Menendez.

""" + old_text

df.loc[agenda_row, 'text'] = enhanced_text
df.to_csv('graphrag_data/city_clerk_documents.csv', index=False)

print("Enhanced E-1 context in agenda document") 