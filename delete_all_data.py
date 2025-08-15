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
# from supabase import create_client  # COMMENTED OUT

load_dotenv()

async def clear_all_data():
    """Clear all records from databases and delete all extracted files."""
    
    print("🗑️  COMPLETE DATA CLEANUP TOOL")
    print("=" * 50)
    print("This will permanently delete:")
    print("• All Cosmos DB graph vertices and edges")
    # print("• All Supabase documents and chunks")  # COMMENTED OUT
    print("• Specific project folders: extracted_json, extracted_markdown, simple_ner_graph")
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
        performed_clear = False
        async with cosmos_client:
            # Get initial counts - EXTRACT THE VALUES FROM THE NESTED LIST
            initial_v_result = await cosmos_client._execute_query("g.V().count()")
            initial_e_result = await cosmos_client._execute_query("g.E().count()")
            
            # FIX: Extract the integer from the NESTED list result [[9397]] -> 9397
            initial_v = initial_v_result[0][0] if initial_v_result and initial_v_result[0] else 0
            initial_e = initial_e_result[0][0] if initial_e_result and initial_e_result[0] else 0
            
            print(f"   Initial state: {initial_v} vertices, {initial_e} edges")
            
            if initial_v > 0 or initial_e > 0:
                # Use the retry-until-empty clear_graph method
                await cosmos_client.clear_graph()
                performed_clear = True
            else:
                print("✅ Cosmos DB was already empty")
        
        # Post-close verification using a fresh client session
        if performed_clear:
            try:
                verify_client = CosmosGraphClient()
                async with verify_client:
                    print("   Performing final verification...")
                    for i in range(3):
                        await asyncio.sleep(2)  # Wait for consistency
                        v_result = await verify_client._execute_query("g.V().count()")
                        e_result = await verify_client._execute_query("g.E().count()")
                        v_count = v_result[0][0] if v_result and v_result[0] else 0
                        e_count = e_result[0][0] if e_result and e_result[0] else 0
                        if v_count > 0 or e_count > 0:
                            print(f"   ⚠️ Verification {i+1}/3: Still found {v_count} vertices, {e_count} edges")
                            # Try one more aggressive delete
                            try:
                                await verify_client._execute_query("g.V().drop()")
                                await verify_client._execute_query("g.E().drop()")
                            except Exception as e:
                                print(f"   Warning during aggressive delete: {e}")
                        else:
                            print(f"   ✅ Verification {i+1}/3: Database is empty")
                            break
                    # Final status check
                    final_v_result = await verify_client._execute_query("g.V().count()")
                    final_e_result = await verify_client._execute_query("g.E().count()")
                    final_v = final_v_result[0][0] if final_v_result and final_v_result[0] else 0
                    final_e = final_e_result[0][0] if final_e_result and final_e_result[0] else 0
                    if final_v == 0 and final_e == 0:
                        print("✅ Cosmos DB confirmed completely empty")
                    else:
                        print(f"❌ FAILED: Cosmos DB still contains data after all attempts")
                        errors.append(f"Cosmos DB: {final_v} vertices and {final_e} edges remain")
            except Exception as e:
                errors.append(f"Cosmos DB verification: {str(e)}")
                print(f"❌ Cosmos DB verification error: {e}")
    except Exception as e:
        errors.append(f"Cosmos DB: {str(e)}")
        print(f"❌ Cosmos DB error: {e}")
    # 2. Clear Supabase - FULLY COMMENTED OUT
    """
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
    """
    
    # 3. Delete specific project folders first
    print("\n3️⃣ Deleting specific project folders...")
    
    specific_folders_to_delete = [
        "/Users/gianmariatroiani/Documents/knologi/graph_database/city_clerk_documents/extracted_json",
        "/Users/gianmariatroiani/Documents/knologi/graph_database/city_clerk_documents/extracted_markdown",
        "/Users/gianmariatroiani/Documents/knologi/graph_database/simple_ner_graph"
    ]
    
    for folder_path in specific_folders_to_delete:
        path = Path(folder_path)
        if path.exists():
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                    print(f"✅ Deleted entire folder: {folder_path}")
                else:
                    print(f"⚠️  {folder_path} is not a directory")
            except Exception as e:
                errors.append(f"{folder_path}: {str(e)}")
                print(f"❌ Error deleting {folder_path}: {e}")
        else:
            print(f"   {folder_path} - not found (skipping)")
    
    # 4. Delete other local extracted files
    print("\n4️⃣ Deleting other local extracted files...")
    
    directories_to_clear = [
        # Main extraction directories
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
    
    # 5. Delete GraphRAG pipeline output files
    print("\n5️⃣ Cleaning up GraphRAG pipeline files...")
    
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
    
    # 6. Delete specific additional files
    print("\n6️⃣ Cleaning up additional files...")
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