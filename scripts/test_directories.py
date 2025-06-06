#!/usr/bin/env python3
"""Test script to verify directory structure."""

from pathlib import Path

def check_directories():
    """Check what directories actually exist."""
    base = Path("city_clerk_documents/global/City Comissions 2024")
    
    print(f"Checking base directory: {base}")
    print(f"Exists: {base.exists()}")
    
    if base.exists():
        print("\nSubdirectories found:")
        for item in base.iterdir():
            if item.is_dir():
                print(f"  📁 {item.name}")
                # Count PDFs
                pdfs = list(item.rglob("*.pdf"))
                print(f"     PDFs: {len(pdfs)}")
                if pdfs and len(pdfs) <= 5:
                    for pdf in pdfs:
                        print(f"       - {pdf.name}")
                elif pdfs:
                    for pdf in pdfs[:3]:
                        print(f"       - {pdf.name}")
                    print(f"       ... and {len(pdfs) - 3} more")

if __name__ == "__main__":
    check_directories() 