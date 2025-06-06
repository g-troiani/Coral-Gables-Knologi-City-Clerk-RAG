# Check what's in the lancedb directory
import os
lancedb_path = 'graphrag_data/output/lancedb'
if os.path.exists(lancedb_path):
    print(f"LanceDB contents:")
    for item in os.listdir(lancedb_path):
        print(f"  - {item}")
else:
    print("LanceDB directory doesn't exist!") 