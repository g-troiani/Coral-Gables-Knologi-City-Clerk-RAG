# Check how documents are being grouped
import yaml
with open('graphrag_data/settings.yaml', 'r') as f:
    settings = yaml.safe_load(f)
    
print("Chunking settings:")
print(f"Size: {settings['chunks']['size']}")
print(f"Overlap: {settings['chunks']['overlap']}")
print(f"Group by: {settings['chunks']['group_by_columns']}") 