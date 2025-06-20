#!/usr/bin/env python3
"""
Delete all records from databases AND all local extracted files.
This includes database records and all processed/extracted content.
"""

import asyncio
import glob
import os
import shutil
from pathlib import Path
from dotenv import load_dotenv
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
from supabase import create_client

load_dotenv()

async def clear_all_data():
    """Clear all records from databases and delete all extracted files."""
    
    print("🗑️  COMPLETE DATA CLEANUP TOOL")
    print("=" * 50)
    print("This will permanently delete:")
    print("• All Cosmos DB graph vertices and edges")
    print("• All Supabase documents and chunks")
    print("• All extracted markdown files")
    print("• All extracted text/JSON files")
    print("• All GraphRAG pipeline output (parquet, lance, cache files)")
    print("• All LanceDB databases and transaction files")
    print("• All temporary files")
    print("\n⚠️  This action CANNOT be undone!")
    
    confirm = input("\nType 'DELETE EVERYTHING' to confirm: ")
    
    if confirm != 'DELETE EVERYTHING':
        print("❌ Operation cancelled")
        return
    
    errors = []
    
    # 1. Clear Cosmos DB
    print("\n1️⃣ Clearing Cosmos DB...")
    try:
        cosmos_client = CosmosGraphClient()
        async with cosmos_client:
            await cosmos_client.clear_graph()
            
            # Verify it's empty
            count = await cosmos_client._execute_query("g.V().count()")
            vertex_count = count[0] if count else 0
            
            if vertex_count == 0:
                print("✅ Cosmos DB cleared successfully")
            else:
                print(f"⚠️  Cosmos DB may not be fully cleared. {vertex_count} vertices remain")
                
    except Exception as e:
        errors.append(f"Cosmos DB: {str(e)}")
        print(f"❌ Cosmos DB error: {e}")
    
    # 2. Clear Supabase
    print("\n2️⃣ Clearing Supabase...")
    try:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not url or not key:
            print("⚠️  Supabase credentials not found - skipping")
        else:
            sb = create_client(url, key)
            
            # Delete chunks first (has foreign key constraint)
            print("   Deleting document chunks...")
            chunks_result = sb.table("documents_chunks").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            
            # Delete documents
            print("   Deleting documents...")
            docs_result = sb.table("city_clerk_documents").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            
            # Verify counts
            doc_count = sb.table("city_clerk_documents").select("id", count="exact").execute()
            chunk_count = sb.table("documents_chunks").select("id", count="exact").execute()
            
            if doc_count.count == 0 and chunk_count.count == 0:
                print("✅ Supabase cleared successfully")
            else:
                print(f"⚠️  Supabase may not be fully cleared:")
                print(f"   Documents remaining: {doc_count.count}")
                print(f"   Chunks remaining: {chunk_count.count}")
                
    except Exception as e:
        errors.append(f"Supabase: {str(e)}")
        print(f"❌ Supabase error: {e}")
    
    # 3. Delete local extracted files
    print("\n3️⃣ Deleting local extracted files...")
    
    directories_to_clear = [
        # Main extraction directories
        "city_clerk_documents/extracted_markdown",
        "city_clerk_documents/extracted_text", 
        "city_clerk_documents/json",
        "city_clerk_documents/txt",
        
        # GraphRAG directories
        "graphrag_data/input",
        "graphrag_data/output",
        "graphrag_data/cache",
        
        # Temporary directories
        "temp_extraction_output",
        "output",
        "reports/embedding"
    ]
    
    for dir_path in directories_to_clear:
        path = Path(dir_path)
        if path.exists():
            try:
                if path.is_dir():
                    # Delete all contents but keep the directory
                    file_count = 0
                    for item in path.iterdir():
                        if item.is_file():
                            item.unlink()
                            file_count += 1
                        elif item.is_dir():
                            shutil.rmtree(item)
                            file_count += 1
                    print(f"✅ Cleared {file_count} items from {dir_path}")
                else:
                    print(f"⚠️  {dir_path} is not a directory")
            except Exception as e:
                errors.append(f"{dir_path}: {str(e)}")
                print(f"❌ Error clearing {dir_path}: {e}")
        else:
            print(f"   {dir_path} - not found (skipping)")
    
    # 4. Delete GraphRAG pipeline output files
    print("\n4️⃣ Cleaning up GraphRAG pipeline files...")
    
    # Delete files with specific GraphRAG patterns
    
    graphrag_patterns = [
        "graphrag_data/**/*_v2",    # Only GraphRAG cache files with hash names
        "**/*.parquet",             # Parquet database files  
        "**/*.lance",               # LanceDB files
        "**/lancedb/**",            # LanceDB directories
        "**/_transactions/**",      # Transaction files
        "**/_versions/**",          # Version files
        "graphrag_data/**/*.json",  # JSON output files
        "graphrag_data/**/*.csv",   # CSV files
        "graphrag_data/**/*.yaml",  # Settings files
    ]
    
    for pattern in graphrag_patterns:
        try:
            matches = list(Path(".").glob(pattern))
            for match in matches:
                if match.exists():
                    try:
                        if match.is_file():
                            match.unlink()
                            print(f"✅ Deleted file: {match}")
                        elif match.is_dir():
                            shutil.rmtree(match)
                            print(f"✅ Deleted directory: {match}")
                    except Exception as e:
                        errors.append(f"{match}: {str(e)}")
                        print(f"❌ Error deleting {match}: {e}")
        except Exception as e:
            errors.append(f"Pattern {pattern}: {str(e)}")
            print(f"❌ Error with pattern {pattern}: {e}")
    
    # 5. Delete specific additional files
    print("\n5️⃣ Cleaning up additional files...")
    additional_files = [
        ".DS_Store",  # macOS system files
        "graphrag_data/context.json",
        "graphrag_data/stats.json",
    ]
    
    for file_path in additional_files:
        path = Path(file_path)
        if path.exists():
            try:
                path.unlink()
                print(f"✅ Deleted {file_path}")
            except Exception as e:
                errors.append(f"{file_path}: {str(e)}")
                print(f"❌ Error deleting {file_path}: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 CLEANUP SUMMARY")
    print("=" * 50)
    
    if not errors:
        print("✅ All data cleared successfully!")
        print("\n💡 The system is now completely reset.")
        print("   To start fresh:")
        print("   1. Run the full pipeline: python -m scripts.graph_rag_stages.main_pipeline")
        print("   2. Or run individual stages as needed")
    else:
        print("⚠️  Some operations failed:")
        for error in errors:
            print(f"   • {error}")
        print("\n💡 You may need to:")
        print("   • Check file permissions")
        print("   • Close any programs using these files")
        print("   • Check your database credentials")

if __name__ == "__main__":
    asyncio.run(clear_all_data()) 