import pandas as pd

# Check if E-1 made it to text units
text_units = pd.read_parquet("graphrag_data/output/text_units.parquet")
e1_in_units = text_units[text_units['text'].str.contains('E-1', case=False, na=False)]
print(f"Text units containing E-1: {len(e1_in_units)}")
if len(e1_in_units) > 0:
    for i, unit in enumerate(e1_in_units['text'].head(3)):
        # Find E-1 context
        import re
        matches = list(re.finditer(r'E-1', unit, re.IGNORECASE))
        for match in matches[:1]:
            start = max(0, match.start() - 100)
            end = min(len(unit), match.end() + 100)
            print(f"\nUnit {i} E-1 context: ...{unit[start:end]}...") 