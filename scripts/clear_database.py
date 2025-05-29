#!/usr/bin/env python3
"""
Database Clear Utility
======================

Safely clears Supabase database tables for the Misophonia Research system.
This script will delete all data from:
- research_documents table
- documents_chunks table

⚠️  WARNING: This operation is irreversible!
"""
from __future__ import annotations
import os
import sys
import logging
from typing import Optional
from gremlin_python.driver import client, serializer
import asyncio
import argparse

from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Cosmos DB configuration
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT", "wss://aida-graph-db.gremlin.cosmos.azure.com:443")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE = os.getenv("COSMOS_DATABASE", "cgGraph")
CONTAINER = os.getenv("COSMOS_CONTAINER", "cityClerk")

if not SUPABASE_URL or not SUPABASE_KEY:
    sys.exit("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

def init_supabase():
    """Initialize Supabase client."""
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_table_counts(sb) -> dict:
    """Get current row counts for all tables."""
    counts = {}
    
    try:
        # Count documents
        doc_res = sb.table("research_documents").select("id", count="exact").execute()
        counts["research_documents"] = doc_res.count or 0
        
        # Count chunks
        chunk_res = sb.table("documents_chunks").select("id", count="exact").execute()
        counts["documents_chunks"] = chunk_res.count or 0
        
        # Count chunks with embeddings
        embedded_res = sb.table("documents_chunks").select("id", count="exact").not_.is_("embedding", "null").execute()
        counts["chunks_with_embeddings"] = embedded_res.count or 0
        
    except Exception as e:
        log.error(f"Error getting table counts: {e}")
        return {}
    
    return counts

def confirm_deletion() -> bool:
    """Ask user for confirmation before deletion."""
    print("\n" + "="*60)
    print("⚠️  DATABASE CLEAR WARNING")
    print("="*60)
    print("This will permanently delete ALL data from:")
    print("  • research_documents table")
    print("  • documents_chunks table")
    print("  • All embeddings and metadata")
    print("\n❌ This operation CANNOT be undone!")
    print("="*60)
    
    response = input("\nType 'DELETE ALL DATA' to confirm (or anything else to cancel): ")
    return response.strip() == "DELETE ALL DATA"

def clear_table_batch(sb, table_name: str, batch_size: int = 1000) -> int:
    """Clear all rows from a specific table in batches to avoid timeouts."""
    log.info(f"Clearing table: {table_name} (batch size: {batch_size})")
    
    total_deleted = 0
    
    while True:
        try:
            # Get a batch of IDs to delete
            result = sb.table(table_name).select("id").limit(batch_size).execute()
            
            if not result.data or len(result.data) == 0:
                break
            
            ids_to_delete = [row["id"] for row in result.data]
            log.info(f"Deleting batch of {len(ids_to_delete)} rows from {table_name}")
            
            # Delete this batch
            delete_result = sb.table(table_name).delete().in_("id", ids_to_delete).execute()
            
            if hasattr(delete_result, 'error') and delete_result.error:
                log.error(f"Error deleting batch from {table_name}: {delete_result.error}")
                break
            
            batch_deleted = len(delete_result.data) if delete_result.data else 0
            total_deleted += batch_deleted
            log.info(f"✅ Deleted {batch_deleted} rows from {table_name} (total: {total_deleted})")
            
            # If we deleted fewer than the batch size, we're done
            if batch_deleted < batch_size:
                break
                
        except Exception as e:
            log.error(f"Exception deleting batch from {table_name}: {e}")
            break
    
    log.info(f"✅ Total deleted from {table_name}: {total_deleted}")
    return total_deleted

def clear_table(sb, table_name: str) -> int:
    """Clear all rows from a specific table."""
    return clear_table_batch(sb, table_name, batch_size=500)

def init_cosmos():
    """Initialize Cosmos DB Gremlin client."""
    if not COSMOS_KEY:
        log.warning("Cosmos DB credentials not found - skipping Cosmos operations")
        return None
    
    try:
        gremlin_client = client.Client(
            f"{COSMOS_ENDPOINT}/gremlin",
            "g",
            username=f"/dbs/{DATABASE}/colls/{CONTAINER}",
            password=COSMOS_KEY,
            message_serializer=serializer.GraphSONSerializersV2d0()
        )
        return gremlin_client
    except Exception as e:
        log.error(f"Failed to connect to Cosmos DB: {e}")
        return None

def get_cosmos_counts(gremlin_client) -> dict:
    """Get current counts for Cosmos DB graph."""
    if not gremlin_client:
        return {}
    
    counts = {}
    try:
        # Count all vertices
        result = gremlin_client.submit("g.V().count()").all()
        counts["total_vertices"] = result[0] if result else 0
        
        # Count by label
        for label in ["Document", "Person", "Meeting", "Chunk"]:
            result = gremlin_client.submit(f"g.V().hasLabel('{label}').count()").all()
            counts[f"{label.lower()}_nodes"] = result[0] if result else 0
        
        # Count edges
        result = gremlin_client.submit("g.E().count()").all()
        counts["total_edges"] = result[0] if result else 0
        
    except Exception as e:
        log.error(f"Error getting Cosmos DB counts: {e}")
        return {}
    
    return counts

def clear_cosmos_graph(gremlin_client) -> tuple[int, int]:
    """Clear all nodes and edges from Cosmos DB graph."""
    if not gremlin_client:
        return 0, 0
    
    log.info("Clearing Cosmos DB graph...")
    
    try:
        # Get initial counts
        edge_result = gremlin_client.submit("g.E().count()").all()
        edge_count = edge_result[0] if edge_result else 0
        
        vertex_result = gremlin_client.submit("g.V().count()").all()
        vertex_count = vertex_result[0] if vertex_result else 0
        
        # Drop all edges first (required before dropping vertices)
        log.info(f"Dropping {edge_count} edges...")
        gremlin_client.submit("g.E().drop()").all()
        
        # Then drop all vertices
        log.info(f"Dropping {vertex_count} vertices...")
        gremlin_client.submit("g.V().drop()").all()
        
        log.info("✅ Cosmos DB graph cleared")
        return vertex_count, edge_count
        
    except Exception as e:
        log.error(f"Error clearing Cosmos DB graph: {e}")
        return 0, 0

async def main():
    """Main function to clear databases."""
    parser = argparse.ArgumentParser(description="Clear City Clerk databases")
    parser.add_argument("--supabase", action="store_true", help="Clear Supabase tables")
    parser.add_argument("--cosmos", action="store_true", help="Clear Cosmos DB graph")
    parser.add_argument("--all", action="store_true", help="Clear all databases")
    
    args = parser.parse_args()
    
    # If no specific database selected, default to all
    if not args.supabase and not args.cosmos and not args.all:
        args.all = True
    
    print("🗑️  City Clerk Database Clear Utility")
    print("=" * 50)
    
    # Initialize connections
    sb = None
    gremlin_client = None
    
    if args.supabase or args.all:
        sb = init_supabase()
    
    if args.cosmos or args.all:
        gremlin_client = init_cosmos()
    
    # Get current counts
    print("\n📊 Current database status:")
    
    supabase_counts = {}
    cosmos_counts = {}
    
    if sb:
        supabase_counts = get_table_counts(sb)
        if supabase_counts:
            print("  Supabase:")
            print(f"    • Documents: {supabase_counts['research_documents']:,}")
            print(f"    • Chunks: {supabase_counts['documents_chunks']:,}")
            print(f"    • Chunks with embeddings: {supabase_counts['chunks_with_embeddings']:,}")
    
    if gremlin_client:
        cosmos_counts = get_cosmos_counts(gremlin_client)
        if cosmos_counts:
            print("  Cosmos DB Graph:")
            print(f"    • Total vertices: {cosmos_counts['total_vertices']:,}")
            print(f"    • Total edges: {cosmos_counts['total_edges']:,}")
            print(f"    • Documents: {cosmos_counts.get('document_nodes', 0):,}")
            print(f"    • Persons: {cosmos_counts.get('person_nodes', 0):,}")
            print(f"    • Meetings: {cosmos_counts.get('meeting_nodes', 0):,}")
    
    # Check if any data exists
    has_data = False
    if sb and supabase_counts:
        has_data = has_data or (supabase_counts['research_documents'] > 0 or supabase_counts['documents_chunks'] > 0)
    if gremlin_client and cosmos_counts:
        has_data = has_data or (cosmos_counts['total_vertices'] > 0)
    
    if not has_data:
        print("\n✅ Databases are already empty!")
        return
    
    # Get confirmation
    if not confirm_deletion():
        print("\n✅ Operation cancelled. Databases unchanged.")
        return
    
    print("\n🗑️  Starting database clear operation...")
    
    # Clear Supabase if requested
    if sb and (args.supabase or args.all):
        print("\n📋 Clearing Supabase tables...")
        chunks_deleted = clear_table(sb, "documents_chunks")
        docs_deleted = clear_table(sb, "research_documents")
        print(f"✅ Supabase cleared: {docs_deleted:,} documents, {chunks_deleted:,} chunks")
    
    # Clear Cosmos DB if requested
    if gremlin_client and (args.cosmos or args.all):
        print("\n🌐 Clearing Cosmos DB graph...")
        vertices_deleted, edges_deleted = clear_cosmos_graph(gremlin_client)
        print(f"✅ Cosmos DB cleared: {vertices_deleted:,} vertices, {edges_deleted:,} edges")
    
    print("\n✅ Database clear operation completed!")

if __name__ == "__main__":
    asyncio.run(main()) 