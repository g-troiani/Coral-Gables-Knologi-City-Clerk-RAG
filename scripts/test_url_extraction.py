#!/usr/bin/env python3
"""
Test script to verify URL extraction from agenda PDFs
"""

import sys
from pathlib import Path
sys.path.append('scripts')

from graph_stages.agenda_pdf_extractor import AgendaPDFExtractor
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def test_url_extraction(pdf_path: str):
    """Test URL extraction on a single PDF."""
    pdf_file = Path(pdf_path)
    
    if not pdf_file.exists():
        log.error(f"PDF file not found: {pdf_path}")
        return
    
    log.info(f"Testing URL extraction on: {pdf_file.name}")
    
    # Initialize extractor
    extractor = AgendaPDFExtractor()
    
    # Extract agenda with URLs
    agenda_data = extractor.extract_agenda(pdf_file)
    
    # Report results
    log.info("\n" + "="*60)
    log.info("EXTRACTION RESULTS")
    log.info("="*60)
    
    # Overall statistics
    hyperlinks = agenda_data.get('hyperlinks', [])
    agenda_items = agenda_data.get('agenda_items', [])
    
    log.info(f"Total hyperlinks found: {len(hyperlinks)}")
    log.info(f"Total agenda items: {len(agenda_items)}")
    
    # Show hyperlinks
    if hyperlinks:
        log.info("\nHYPERLINKS FOUND:")
        for i, link in enumerate(hyperlinks[:10]):  # Show first 10
            log.info(f"  {i+1}. Text: {link.get('text', 'N/A')[:50]}...")
            log.info(f"     URL: {link.get('url', 'N/A')}")
            log.info(f"     Page: {link.get('page', 'N/A')}")
        
        if len(hyperlinks) > 10:
            log.info(f"  ... and {len(hyperlinks) - 10} more")
    
    # Show items with URLs
    items_with_urls = [item for item in agenda_items if item.get('urls')]
    
    if items_with_urls:
        log.info(f"\nAGENDA ITEMS WITH URLS: {len(items_with_urls)}")
        for item in items_with_urls[:5]:  # Show first 5
            log.info(f"\n  Item: {item.get('item_code')} - {item.get('document_reference')}")
            log.info(f"  Title: {item.get('title', 'N/A')[:100]}...")
            log.info(f"  URLs:")
            for url in item.get('urls', []):
                log.info(f"    - {url.get('text', 'Link')[:50]}... -> {url.get('url', 'N/A')[:50]}...")
    
    # Save detailed results
    output_file = pdf_file.parent / f"{pdf_file.stem}_url_test_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            'pdf_file': pdf_file.name,
            'total_hyperlinks': len(hyperlinks),
            'total_agenda_items': len(agenda_items),
            'items_with_urls': len(items_with_urls),
            'hyperlinks': hyperlinks,
            'agenda_items_with_urls': items_with_urls
        }, f, indent=2)
    
    log.info(f"\nDetailed results saved to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_url_extraction(sys.argv[1])
    else:
        # Test with a default agenda from the actual directory structure
        test_path = "city_clerk_documents/global/City Comissions 2024/Agendas/Agenda 01.9.2024.pdf"
        if Path(test_path).exists():
            test_url_extraction(test_path)
        else:
            # Try alternative paths
            alt_paths = [
                "city_clerk_documents/global copy/City Comissions 2024/Agendas/Agenda 01.9.2024.pdf",
                Path.cwd() / "city_clerk_documents/global/City Comissions 2024/Agendas/Agenda 01.9.2024.pdf"
            ]
            
            for alt_path in alt_paths:
                if Path(alt_path).exists():
                    test_url_extraction(str(alt_path))
                    break
            else:
                log.error("Could not find agenda PDF in expected locations")
                log.error("Please provide a PDF path as argument")
                log.error("Usage: python test_url_extraction.py <path_to_pdf>")
                log.error("\nExpected locations:")
                log.error("  - city_clerk_documents/global/City Comissions 2024/Agendas/")
                log.error("  - city_clerk_documents/global copy/City Comissions 2024/Agendas/") 