# Check what's in settings.yaml
import yaml
with open('graphrag_data/settings.yaml', 'r') as f:
    settings = yaml.safe_load(f)
    
print("Embedding settings:")
if 'embeddings' in settings:
    print("Legacy format:", settings['embeddings'])
if 'models' in settings and 'default_embedding_model' in settings['models']:
    print("Modern format:", settings['models']['default_embedding_model']) 