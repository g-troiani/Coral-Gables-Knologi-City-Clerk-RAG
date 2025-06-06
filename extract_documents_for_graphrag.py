#!/usr/bin/env python3
"""
Extract documents using enhanced PDF extractor for GraphRAG processing.
This script uses the intelligent metadata header functionality.
"""

import sys
from pathlib import Path
import asyncio

# Add current directory to path
sys.path.append('.')

from scripts.graph_stages.pdf_extractor import PDFExtractor

def extract_documents_for_graphrag():
    """Extract a few sample documents for GraphRAG testing."""
    
    print("🚀 Starting document extraction for GraphRAG testing")
    print("="*60)
    
    # Define source and output directories
    pdf_dir = Path("city_clerk_documents/global copy/City Comissions 2024/Agendas")
    output_dir = Path("city_clerk_documents/extracted_text")
    markdown_dir = Path("city_clerk_documents/extracted_markdown")
    
    # Create output directories
    output_dir.mkdir(parents=True, exist_ok=True)
    markdown_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Source directory: {pdf_dir}")
    print(f"📁 JSON output directory: {output_dir}")
    print(f"📁 Markdown output directory: {markdown_dir}")
    
    # Find agenda PDFs (limit to a few for testing)
    agenda_files = sorted(pdf_dir.glob("Agenda*.pdf"))[:3]  # Just first 3 for testing
    
    if not agenda_files:
        print("❌ No agenda PDFs found!")
        return False
    
    print(f"📄 Found {len(agenda_files)} agenda files to process:")
    for pdf in agenda_files:
        print(f"   - {pdf.name}")
    
    # Initialize extractor
    extractor = PDFExtractor(pdf_dir, output_dir)
    
    # Process each PDF
    for pdf_path in agenda_files:
        try:
            print(f"\n📄 Processing: {pdf_path.name}")
            
            # Extract with intelligent metadata headers
            markdown_path, enhanced_content = extractor.extract_and_save_with_metadata(
                pdf_path, markdown_dir
            )
            print(f"✅ Enhanced markdown saved to: {markdown_path}")
            
            # Also extract regular JSON for compatibility
            full_text, pages = extractor.extract_text_from_pdf(pdf_path)
            extracted_data = {
                'full_text': full_text,
                'pages': pages,
                'document_type': extractor._determine_doc_type(pdf_path.name),
                'metadata': {
                    'filename': pdf_path.name,
                    'num_pages': len(pages),
                    'total_chars': len(full_text),
                    'extraction_method': 'docling_enhanced'
                }
            }
            
            # Save JSON
            import json
            json_path = output_dir / f"{pdf_path.stem}_extracted.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            print(f"✅ JSON saved to: {json_path}")
            
        except Exception as e:
            print(f"❌ Error processing {pdf_path.name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n✅ Document extraction complete!")
    print(f"📊 Extracted {len(agenda_files)} documents")
    print(f"📁 Files available in:")
    print(f"   - JSON: {output_dir}")
    print(f"   - Enhanced Markdown: {markdown_dir}")
    
    return True

if __name__ == "__main__":
    success = extract_documents_for_graphrag()
    if success:
        print("\n🎯 Ready for GraphRAG pipeline!")
    else:
        print("\n❌ Extraction failed")
        sys.exit(1) 