"""
Check City Clerk Pipeline Setup
"""
from pathlib import Path
import os

def check_setup():
    """Check if the pipeline environment is properly set up."""
    print("🔍 Checking City Clerk Pipeline Setup\n")
    
    # Check current directory
    cwd = Path.cwd()
    print(f"📂 Current directory: {cwd}")
    print(f"📂 Script location: {Path(__file__).parent}")
    
    # Check for agenda directory
    print("\n📁 Checking for agenda files:")
    possible_dirs = [
        Path("city_clerk_documents/global"),
        Path("../city_clerk_documents/global"),
        Path("../../city_clerk_documents/global"),
        cwd / "city_clerk_documents" / "global"
    ]
    
    found_dir = None
    for dir_path in possible_dirs:
        abs_path = dir_path.absolute()
        exists = dir_path.exists()
        print(f"   {dir_path} -> {abs_path}")
        print(f"   Exists: {exists}")
        
        if exists:
            files = list(dir_path.glob("*.pdf"))
            agenda_files = list(dir_path.glob("*genda*.pdf"))
            print(f"   Total PDFs: {len(files)}")
            print(f"   Agenda PDFs: {len(agenda_files)}")
            
            if agenda_files:
                print(f"   Found agenda files:")
                for f in agenda_files[:5]:
                    print(f"      - {f.name}")
                found_dir = dir_path
                break
        print()
    
    if found_dir:
        print(f"✅ Found agenda directory: {found_dir}")
        print(f"\n💡 Run the pipeline with:")
        print(f"   python scripts/graph_pipeline.py --agenda-dir '{found_dir}'")
    else:
        print("❌ Could not find agenda directory!")
        print("\n💡 Please ensure:")
        print("   1. You have the city_clerk_documents/global directory")
        print("   2. It contains PDF files with 'Agenda' in the name")
        print("   3. You're running from the correct directory")
    
    # Check environment variables
    print("\n🔑 Checking environment variables:")
    env_vars = ['COSMOS_ENDPOINT', 'COSMOS_KEY', 'GROQ_API_KEY']
    all_set = True
    for var in env_vars:
        value = os.getenv(var)
        if value:
            print(f"   ✅ {var}: {'*' * 10} (set)")
        else:
            print(f"   ❌ {var}: NOT SET")
            all_set = False
    
    if not all_set:
        print("\n💡 Create a .env file with the missing variables")
    
    # Check Python imports
    print("\n📦 Checking Python imports:")
    try:
        import gremlin_python
        print("   ✅ gremlin_python")
    except ImportError:
        print("   ❌ gremlin_python - run: pip install gremlinpython")
    
    try:
        import groq
        print("   ✅ groq")
    except ImportError:
        print("   ❌ groq - run: pip install groq")
    
    try:
        import fitz
        print("   ✅ PyMuPDF (fitz)")
    except ImportError:
        print("   ❌ PyMuPDF - run: pip install PyMuPDF")
    
    try:
        import unstructured
        print("   ✅ unstructured")
    except ImportError:
        print("   ❌ unstructured - run: pip install unstructured")

if __name__ == "__main__":
    check_setup() 