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

from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

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

def main():
    """Main function to clear the database."""
    print("🗑️  Misophonia Database Clear Utility")
    print("=" * 50)
    
    # Initialize Supabase
    sb = init_supabase()
    
    # Get current counts
    print("\n📊 Current database status:")
    counts = get_table_counts(sb)
    
    if not counts:
        print("❌ Could not retrieve database counts. Exiting.")
        return
    
    print(f"  • Documents: {counts['research_documents']:,}")
    print(f"  • Chunks: {counts['documents_chunks']:,}")
    print(f"  • Chunks with embeddings: {counts['chunks_with_embeddings']:,}")
    
    if counts['research_documents'] == 0 and counts['documents_chunks'] == 0:
        print("\n✅ Database is already empty!")
        return
    
    # Get confirmation
    if not confirm_deletion():
        print("\n✅ Operation cancelled. Database unchanged.")
        return
    
    print("\n🗑️  Starting database clear operation...")
    
    # Clear chunks first (has foreign key to documents)
    chunks_deleted = clear_table(sb, "documents_chunks")
    
    # Clear documents
    docs_deleted = clear_table(sb, "research_documents")
    
    # Verify deletion
    print("\n📊 Verifying deletion...")
    final_counts = get_table_counts(sb)
    
    if final_counts:
        print(f"  • Documents remaining: {final_counts['research_documents']:,}")
        print(f"  • Chunks remaining: {final_counts['documents_chunks']:,}")
        
        if final_counts['research_documents'] == 0 and final_counts['documents_chunks'] == 0:
            print("\n✅ Database successfully cleared!")
            print(f"  • Deleted {docs_deleted:,} documents")
            print(f"  • Deleted {chunks_deleted:,} chunks")
        else:
            print("\n⚠️  Some data may remain in the database.")
    else:
        print("❌ Could not verify deletion status.")

if __name__ == "__main__":
    main() 