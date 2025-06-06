# Quick script to view results
from pathlib import Path
import pandas as pd

output_dir = Path("graphrag_data/output")
entities_df = pd.read_parquet(output_dir / "entities.parquet")
print(f"Entities: {len(entities_df)}")
print(entities_df.head()) 