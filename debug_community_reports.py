# Check community reports - these should have content
import pandas as pd

reports = pd.read_parquet("graphrag_data/output/community_reports.parquet")
print(f"\nCommunity reports: {len(reports)}")
if len(reports) > 0:
    print("Sample report:", reports.iloc[0]['summary'][:200] if 'summary' in reports.columns else "No summary column") 