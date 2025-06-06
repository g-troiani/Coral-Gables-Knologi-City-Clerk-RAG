# Update settings.yaml to ensure embeddings are configured
import yaml

with open('graphrag_data/settings.yaml', 'r') as f:
    settings = yaml.safe_load(f)

# Ensure embedding model is configured
if 'models' not in settings:
    settings['models'] = {}

settings['models']['default_embedding_model'] = {
    'type': 'openai_embedding',
    'api_key': '${OPENAI_API_KEY}',
    'model': 'text-embedding-3-small',
    'batch_size': 16,
    'batch_max_tokens': 2048
}

# Also add legacy format for compatibility
settings['embeddings'] = {
    'api_key': '${OPENAI_API_KEY}',
    'model': 'text-embedding-3-small',
    'batch_size': 16,
    'batch_max_tokens': 2048
}

with open('graphrag_data/settings.yaml', 'w') as f:
    yaml.dump(settings, f, sort_keys=False)

print("Updated embedding configuration") 