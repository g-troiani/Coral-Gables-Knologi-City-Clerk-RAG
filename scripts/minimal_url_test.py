#!/usr/bin/env python3
"""
Minimal test to verify PyMuPDF can extract URLs from agenda PDFs
Run this first to ensure PyMuPDF is working correctly
"""

import fitz  # PyMuPDF
from pathlib import Path
import sys

def test_pymupdf_urls(pdf_path):
    """Quick test of PyMuPDF URL extraction."""
    print(f"\nTesting PyMuPDF on: {pdf_path}")
    print("-" * 60)
    
    try:
        # Open PDF
        doc = fitz.open(pdf_path)
        print(f"✅ Opened PDF with {len(doc)} pages")
        
        # Check each page for links
        total_links = 0
        for page_num in range(len(doc)):
            page = doc[page_num]
            links = page.get_links()
            
            if links:
                print(f"\n📄 Page {page_num + 1}: Found {len(links)} links")
                for i, link in enumerate(links[:3]):  # Show first 3
                    if link.get('uri'):
                        rect = fitz.Rect(link['from'])
                        text = page.get_text(clip=rect).strip()
                        print(f"   Link {i+1}: {text[:30]}... → {link['uri'][:50]}...")
                        total_links += 1
        
        doc.close()
        
        print(f"\n✅ Total URLs found: {total_links}")
        return total_links > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    # Default test path
    test_path = "city_clerk_documents/global/City Comissions 2024/Agendas/Agenda 01.9.2024.pdf"
    
    if len(sys.argv) > 1:
        test_path = sys.argv[1]
    
    if Path(test_path).exists():
        success = test_pymupdf_urls(test_path)
        print(f"\n{'✅ SUCCESS' if success else '❌ FAILED'}: PyMuPDF URL extraction")
    else:
        print(f"❌ File not found: {test_path}")
        print("Usage: python minimal_url_test.py [path_to_pdf]") 