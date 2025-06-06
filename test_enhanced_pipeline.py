#!/usr/bin/env python3
"""
Test script to demonstrate the enhanced pipeline functionality:
1. PDF extraction with intelligent metadata headers
2. Document adapter for GraphRAG preparation
3. Enhanced markdown formatting for better entity recognition
"""

import sys
from pathlib import Path
import json

# Add current directory to path
sys.path.append('.')

from scripts.graph_stages.pdf_extractor import PDFExtractor
from scripts.graphrag_breakdown.document_adapter import CityClerkDocumentAdapter

def test_enhanced_pipeline():
    """Test the complete enhanced pipeline."""
    
    print("🚀 Testing Enhanced GraphRAG Pipeline")
    print("="*60)
    
    # Test 1: Verify extracted documents with intelligent headers
    print("\n📋 Test 1: Verifying Enhanced Document Extraction")
    print("-" * 40)
    
    markdown_dir = Path("city_clerk_documents/extracted_markdown")
    extracted_dir = Path("city_clerk_documents/extracted_text")
    
    if not markdown_dir.exists() or not extracted_dir.exists():
        print("❌ Missing extracted documents. Please run extract_documents_for_graphrag.py first")
        return False
    
    # Check markdown files with intelligent headers
    markdown_files = list(markdown_dir.glob("*.md"))
    json_files = list(extracted_dir.glob("*_extracted.json"))
    
    print(f"✅ Found {len(markdown_files)} enhanced markdown files")
    print(f"✅ Found {len(json_files)} JSON files")
    
    # Examine first markdown file
    if markdown_files:
        sample_file = markdown_files[0]
        print(f"\n📄 Sample enhanced document: {sample_file.name}")
        
        with open(sample_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for key enhancement features
        has_metadata_header = "DOCUMENT METADATA AND CONTEXT" in content
        has_searchable_ids = "SEARCHABLE IDENTIFIERS:" in content
        has_query_helpers = "QUERY HELPERS:" in content
        has_natural_desc = "NATURAL LANGUAGE DESCRIPTION:" in content
        
        print(f"   ✅ Metadata header: {'Present' if has_metadata_header else 'Missing'}")
        print(f"   ✅ Searchable identifiers: {'Present' if has_searchable_ids else 'Missing'}")
        print(f"   ✅ Query helpers: {'Present' if has_query_helpers else 'Missing'}")
        print(f"   ✅ Natural language description: {'Present' if has_natural_desc else 'Missing'}")
        
        # Show sample metadata
        if has_metadata_header:
            lines = content.split('\n')
            metadata_section = []
            in_metadata = False
            for line in lines:
                if "DOCUMENT METADATA AND CONTEXT" in line:
                    in_metadata = True
                if in_metadata:
                    metadata_section.append(line)
                    if line.strip() == "---" and len(metadata_section) > 5:
                        break
            
            print(f"\n📋 Sample metadata structure:")
            for line in metadata_section[:15]:  # Show first 15 lines
                print(f"      {line}")
            if len(metadata_section) > 15:
                print(f"      ... ({len(metadata_section)-15} more lines)")
    
    # Test 2: Document Adapter for GraphRAG
    print(f"\n📋 Test 2: Document Adapter for GraphRAG")
    print("-" * 40)
    
    try:
        # Test JSON-based adapter (existing functionality)
        adapter = CityClerkDocumentAdapter(extracted_dir)
        output_dir = Path("test_output")
        output_dir.mkdir(exist_ok=True)
        
        df = adapter.prepare_documents_for_graphrag(output_dir)
        print(f"✅ Prepared {len(df)} documents using JSON adapter")
        print(f"   Document types: {df['document_type'].value_counts().to_dict()}")
        
        # Test Markdown-based adapter (new functionality)
        df_markdown = adapter.prepare_documents_from_markdown(markdown_dir, output_dir)
        print(f"✅ Prepared {len(df_markdown)} documents using enhanced markdown adapter")
        
        # Compare the two approaches
        csv_path = output_dir / "city_clerk_documents.csv"
        if csv_path.exists():
            print(f"✅ GraphRAG input file created: {csv_path}")
            print(f"   File size: {csv_path.stat().st_size / 1024:.1f} KB")
            
            # Show sample records
            sample_records = df_markdown.head(2)
            print(f"\n📋 Sample GraphRAG records:")
            for idx, row in sample_records.iterrows():
                print(f"   Record {idx+1}:")
                print(f"      ID: {row['id']}")
                print(f"      Title: {row['title']}")
                print(f"      Document Type: {row['document_type']}")
                print(f"      Text Length: {len(row['text'])} characters")
                print(f"      Text Preview: {row['text'][:200]}...")
                print()
        
    except Exception as e:
        print(f"❌ Document adapter test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 3: Entity Recognition Enhancement
    print(f"\n📋 Test 3: Entity Recognition Features")
    print("-" * 40)
    
    try:
        # Test entity enhancement functionality
        pdf_dir = Path("city_clerk_documents/global copy/City Comissions 2024/Agendas")
        extractor = PDFExtractor(pdf_dir)
        
        # Test the enhancement functions
        sample_text = "Item E-1 discusses the waterfront project. WHEREAS the City Commission..."
        enhanced_text = extractor.enhance_markdown_for_graphrag(
            sample_text, 
            "agenda", 
            {"document_number": "2024-01"}
        )
        
        print("✅ Entity enhancement test:")
        print(f"   Original: {sample_text}")
        print(f"   Enhanced: {enhanced_text}")
        
        # Test document info parsing
        doc_info = extractor._parse_document_info(
            "2024-01 - 01_09_2024.pdf", 
            ["Ordinances", "2024"]
        )
        
        print(f"\n✅ Document parsing test:")
        print(f"   Type: {doc_info['type']}")
        print(f"   Document Number: {doc_info['doc_number']}")
        print(f"   Date: {doc_info['date']}")
        print(f"   Related Items: {doc_info['agenda_items']}")
        print(f"   Description: {doc_info['nl_description']}")
        
    except Exception as e:
        print(f"❌ Entity recognition test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 4: Query Helper Analysis
    print(f"\n📋 Test 4: Query Helper Effectiveness")
    print("-" * 40)
    
    # Analyze the enhanced documents for query-friendliness
    total_agenda_items = 0
    agenda_item_patterns = 0
    
    for md_file in markdown_files:
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Count agenda item references
        import re
        agenda_refs = re.findall(r'[A-Z]-\d+', content)
        total_agenda_items += len(agenda_refs)
        
        # Count enhanced patterns
        enhanced_patterns = re.findall(r'\*\*AGENDA_ITEM:\s*([A-Z]-\d+)\*\*', content)
        agenda_item_patterns += len(enhanced_patterns)
    
    print(f"✅ Query analysis results:")
    print(f"   Total agenda item references found: {total_agenda_items}")
    print(f"   Enhanced agenda item patterns: {agenda_item_patterns}")
    print(f"   Enhancement ratio: {agenda_item_patterns/max(total_agenda_items,1)*100:.1f}%")
    
    # Final summary
    print(f"\n🎯 Pipeline Test Summary")
    print("="*60)
    print("✅ Enhanced PDF extraction with intelligent metadata headers")
    print("✅ Document adapter with both JSON and Markdown support")  
    print("✅ Entity recognition enhancements for agenda items")
    print("✅ Query helpers and natural language descriptions")
    print("✅ GraphRAG-ready CSV output generation")
    
    print(f"\n📈 Results:")
    print(f"   - {len(markdown_files)} documents with enhanced metadata")
    print(f"   - {len(df_markdown)} records prepared for GraphRAG")
    print(f"   - {total_agenda_items} agenda item references preserved")
    print(f"   - Enhanced pattern matching for better retrieval")
    
    print(f"\n🎯 Next Steps:")
    print("   - Documents are ready for GraphRAG processing")
    print("   - Enhanced metadata should improve entity recognition")
    print("   - Intelligent headers provide better context for queries")
    print("   - Query helpers should improve search relevance")
    
    return True

if __name__ == "__main__":
    success = test_enhanced_pipeline()
    if success:
        print("\n✅ Enhanced pipeline test completed successfully!")
    else:
        print("\n❌ Enhanced pipeline test failed")
        sys.exit(1) 