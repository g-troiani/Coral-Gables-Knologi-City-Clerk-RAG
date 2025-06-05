from pathlib import Path
import pandas as pd
import json
from typing import Dict, List, Any
import asyncio
from scripts.graph_stages.cosmos_db_client import CosmosGraphClient

class GraphRAGCosmosSync:
    """Synchronize GraphRAG output with Cosmos DB."""
    
    def __init__(self, graphrag_output_dir: Path):
        self.output_dir = Path(graphrag_output_dir)
        self.cosmos_client = CosmosGraphClient()
        
    async def sync_to_cosmos(self):
        """Sync GraphRAG data to Cosmos DB."""
        await self.cosmos_client.connect()
        
        try:
            # Load GraphRAG artifacts
            entities_df = pd.read_parquet(self.output_dir / "entities.parquet")
            relationships_df = pd.read_parquet(self.output_dir / "relationships.parquet")
            communities_df = pd.read_parquet(self.output_dir / "communities.parquet")
            
            # Sync entities
            print(f"📤 Syncing {len(entities_df)} entities to Cosmos DB...")
            for _, entity in entities_df.iterrows():
                await self._sync_entity(entity)
            
            # Sync relationships
            print(f"🔗 Syncing {len(relationships_df)} relationships...")
            for _, rel in relationships_df.iterrows():
                await self._sync_relationship(rel)
            
            # Sync communities as properties
            print(f"🏘️ Syncing {len(communities_df)} communities...")
            for _, community in communities_df.iterrows():
                await self._sync_community(community)
                
        finally:
            await self.cosmos_client.close()
    
    async def _sync_entity(self, entity: pd.Series):
        """Sync a GraphRAG entity to Cosmos DB."""
        # Map GraphRAG entity to Cosmos vertex
        vertex_id = f"graphrag_entity_{entity['id']}"
        
        properties = {
            'name': entity['name'],
            'type': entity['type'],
            'description': entity['description'],
            'graphrag_id': entity['id'],
            'community_ids': json.dumps(entity.get('community_ids', [])),
            'has_graphrag': True
        }
        
        # Map to appropriate label based on type
        label_map = {
            'person': 'Person',
            'organization': 'Organization',
            'location': 'Location',
            'document': 'Document',
            'meeting': 'Meeting',
            'agenda_item': 'AgendaItem',
            'project': 'Project'
        }
        
        label = label_map.get(entity['type'].lower(), 'Entity')
        
        await self.cosmos_client.upsert_vertex(
            label=label,
            vertex_id=vertex_id,
            properties=properties
        )
    
    async def _sync_relationship(self, rel: pd.Series):
        """Sync a GraphRAG relationship to Cosmos DB."""
        from_id = f"graphrag_entity_{rel['source']}"
        to_id = f"graphrag_entity_{rel['target']}"
        
        properties = {
            'description': rel['description'],
            'weight': rel.get('weight', 1.0),
            'graphrag_rel_id': rel['id']
        }
        
        await self.cosmos_client.create_edge_if_not_exists(
            from_id=from_id,
            to_id=to_id,
            edge_type=rel['type'].upper(),
            properties=properties
        )
    
    async def _sync_community(self, community: pd.Series):
        """Sync a GraphRAG community as metadata to relevant entities."""
        # Communities can be stored as properties on entities
        # or as separate vertices depending on your schema preference
        pass 