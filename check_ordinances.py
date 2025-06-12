import pandas as pd

# Check what ordinances were extracted
entities = pd.read_parquet('graphrag_data/output/entities.parquet')

# First check the columns
print("Columns in entities:", entities.columns.tolist())
print("Sample row:")
print(entities.head(1))

# Look for ordinances
ordinances = entities[entities['type'] == 'ORDINANCE']
print(f"\nOrdinances extracted: {len(ordinances)}")
print(ordinances[['title', 'description']].head(10))

# Also check for 2024-01 in any entity
ord_2024_01 = entities[entities['title'].str.contains('2024-01', case=False, na=False) | 
                       entities['description'].str.contains('2024-01', case=False, na=False)]
print(f"\n2024-01 mentions: {len(ord_2024_01)}")
if len(ord_2024_01) > 0:
    print(ord_2024_01[['title', 'type', 'description']])

# Check if "ORDINANCE 3576" was extracted (that's the Cocoplum ordinance)
ord_3576 = entities[entities['title'].str.contains('3576', na=False)]
print(f"\nOrdinance 3576 found: {len(ord_3576)}")
if len(ord_3576) > 0:
    print(ord_3576[['title', 'type', 'description']].iloc[0])
    print(f"\nFull description of first 3576 ordinance:")
    print(ord_3576.iloc[0]['description'])

# Check if Ordinance 3576 is the E-1 Cocoplum ordinance
if len(ord_3576) > 0:
    desc = ord_3576.iloc[0]['description']
    if 'Cocoplum' in desc or 'E-1' in desc:
        print("\n✅ Ordinance 3576 IS the Cocoplum/E-1 ordinance!")
        print(f"Description: {desc}")
    else:
        print("\n❌ Ordinance 3576 does not appear to be the Cocoplum/E-1 ordinance")
        print(f"Description: {desc}") 