#!/usr/bin/env python3
import sys, json
sys.path.append('scripts')
from graph_stages.ontology_extractor import OntologyExtractor
from pathlib import Path

# Test the ontology extractor
extractor = OntologyExtractor()
result = extractor.extract_ontology(Path('city_clerk_documents/global/City Comissions 2024/Agendas/Agenda 01.9.2024.pdf'))

# Find E-1 and check its URLs
found = False
for section in result.get('sections', []):
    for item in section.get('items', []):
        if item.get('item_code') == 'E-1':
            urls = item.get('urls', [])
            print(f'✅ E-1 found with {len(urls)} URLs: {urls}')
            found = True
            break
    if found:
        break

if not found:
    print('❌ E-1 not found in ontology') 