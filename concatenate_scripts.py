import os
import sys
import datetime
import re
import fnmatch

# --- Configuration Constants ---

# Define allowed file extensions and specific filenames
ALLOWED_EXTENSIONS = [
    '.js', '.jsx', '.html', '.css', '.py', '.md', 
    '.json', '.toml', '.yaml', '.yml', '.gitignore'
]
ALLOWED_FILENAMES = [
    'requirements.txt',
    'setup.py',
    'pyproject.toml',
    'Dockerfile',
    'docker-compose.yml',
    'docker-compose.yaml'
]

# Variables for files and directories to exclude
OUTPUT_FILENAME_TEMPLATE = 'concatenated_scripts_part{}.txt'
# Dynamically get the name of the script file itself
SCRIPT_FILENAME = os.path.basename(sys.argv[0]) 

EXCLUDED_FILES = [
    'concatenated_scripts_part1.txt',
    'concatenated_scripts_part2.txt',
    'concatenated_scripts_part3.txt',
    SCRIPT_FILENAME, # Exclude the script file itself
    '.env', # Exclude environment variable files
    '.DS_Store', # macOS system file
    'city_clerk_graph.html', # Exclude specific HTML file
    # RAG Pipeline Files - Exclude entire RAG system
    'rag_local_web_app.py',
    'pipeline_modular_optimized.py',
    'supabase_clear_database.py',
    'test_vector_search.py',
    'find_duplicates.py',
    'topic_filter_and_title.py',
    # JSON output files - common patterns
    'output.json',
    'results.json',
    'processed.json',
    'extracted.json',
    'data.json',
    'cache.json',
    'temp.json',
    'backup.json',
    'export.json',
    'report.json',
    'log.json',
    'response.json',
    'api-response.json',
    'processed_documents.json',
    'extracted_text.json',
    'vectorstore.json',
    'embeddings.json',
    'index.json',
    'metadata.json',
    'processed_metadata.json',
    # Library and version files
    'package-lock.json',
    'yarn.lock',
    'composer.lock',
    'Pipfile.lock',
    'poetry.lock',
    'pnpm-lock.yaml',
    'npm-shrinkwrap.json',
    'bower.json',
    'component.json',
    # Virtual environment files
    'pyvenv.cfg',
    'activate',
    'activate.bat',
    'activate.ps1',
    'activate.fish',
    'activate.csh',
    'pip-selfcheck.json',
    # IDE and editor files
    '.vscode',
    '.idea',
    'Thumbs.db',
    'Desktop.ini',
    # Coverage and test files
    '.coverage',
    '.nyc_output',
    'coverage.xml',
    '.hypothesis',
    '.pytest_cache',
    # Documentation files that are typically very long
    'CHANGELOG.md',
    'CHANGELOG.txt',
    'HISTORY.md',
    'HISTORY.txt',
    'LICENSE',
    'LICENSE.txt',
    'LICENSE.md',
    'COPYING',
    'NOTICE',
    'NOTICE.txt',
    'AUTHORS',
    'AUTHORS.txt',
    'CONTRIBUTORS',
    'CONTRIBUTORS.txt',
    'INSTALL',
    'INSTALL.txt',
    'INSTALL.md',
]

# Expanded list of exclusions for virtual environments and node modules
EXCLUDED_DIRS = [
    '__pycache__',
    '.git',
    'node_modules',       # Node modules
    'dist',               # Build output
    '.netlify',           # Netlify directory
    'venv',               # Common Python virtual env name
    '.venv',              # Another common virtual env name
    'env',                # Another common virtual env name
    'virtualenv',         # Another virtual env name
    'city_clerk_rag',     # Specific virtual env folder for this project
    'city-clerk-rag',     # Alternative naming
    'city_clerk_env',     # Potential virtual env name
    'city-clerk-env',     # Potential virtual env name
    'cache',              # Cache directories
    'artifacts',          # Generated artifacts
    'reports',            # Report files
    'logs',               # Log files
    'temp',               # Temporary files
    'tmp',                # Temporary files
    'city_clerk_documents/global',  # Source PDFs directory
    'city_clerk_documents/txt',     # Extracted text files
    'city_clerk_documents/json',    # Extracted JSON files
    'documents/',
    # RAG Pipeline Directories - Exclude entire RAG system
    'stages',             # RAG pipeline stages directory
    'scripts/stages',     # Full path to RAG stages
    # Library and vendor directories
    'lib',                # Library directories
    'libs',               # Library directories
    'vendor',             # Vendor/third-party code
    'vendors',            # Vendor directories
    'third-party',        # Third-party libraries
    'third_party',        # Third-party libraries
    'site-packages',      # Python site packages
    'include',            # C/C++ include directories
    'bin',                # Binary directories
    'build',              # Build directories
    'target',             # Build target directories
    '.pytest_cache',      # Pytest cache
    '.coverage',          # Coverage files
    '.mypy_cache',        # MyPy cache
    '.tox',               # Tox environments
    'htmlcov',            # Coverage HTML reports
    'coverage',           # Coverage directories
    # Documentation that's typically long
    'docs',               # Documentation
    'documentation',      # Documentation
    'examples',           # Example code (often not needed)
    'samples',            # Sample code
    'test',               # Test directories
    'tests',              # Test directories
    'testing',            # Testing directories
    '__tests__',          # Jest tests
    'spec',               # Spec files
    'specs',              # Spec files
]

# Path-based exclusions - these are specific paths we want to exclude
EXCLUDED_PATHS = [
    # Add any specific paths that should be excluded for city clerk RAG
]

# Essential documentation files that contain architectural information
ESSENTIAL_DOCS = [
    'README.md',           # Main project README if it exists
    'config.py',           # Configuration files are important
    'settings.py',         # Settings files
]

# Additional patterns to identify virtual environments
VENV_PATTERNS = [
    'venv', 'virtualenv', 'env', 'python3', 'python', 'city_clerk_rag', 'city-clerk-rag'
]

# --- Helper Functions ---

def is_venv_or_node_modules(path):
    """
    More robust check for virtual environments and node_modules.
    Returns True if the path appears to be a virtual environment or node_modules.
    """
    path_lower = path.lower()
    path_parts = os.path.normpath(path).split(os.sep)
    
    # Check for node_modules
    if 'node_modules' in path_parts:
        return True
    
    # Check if this directory itself has virtual environment indicators
    if os.path.exists(os.path.join(path, 'pyvenv.cfg')) or \
       os.path.exists(os.path.join(path, 'bin', 'activate')) or \
       os.path.exists(os.path.join(path, 'Scripts', 'activate.bat')) or \
       os.path.exists(os.path.join(path, 'lib', 'python')):
        return True
    
    # Check for common virtual environment patterns, but only if the directory name itself matches
    directory_name = os.path.basename(path).lower()
    for pattern in VENV_PATTERNS:
        if directory_name == pattern or directory_name.startswith(pattern + '_') or directory_name.startswith(pattern + '-'):
            # Additional check - does it contain typical venv structures?
            if os.path.exists(os.path.join(path, 'bin', 'activate')) or \
               os.path.exists(os.path.join(path, 'Scripts', 'activate.bat')) or \
               os.path.exists(os.path.join(path, 'lib', 'python')):
                return True
    
    return False

def is_library_or_unnecessary_file(file_path, filename):
    """
    Determines if a file is a library file or unnecessarily long content that should be excluded.
    Returns True if the file should be excluded.
    """
    # Check for common library file patterns
    library_patterns = [
        'jquery', 'bootstrap', 'lodash', 'moment', 'axios', 'react', 'vue', 'angular',
        'webpack', 'babel', 'eslint', 'prettier', 'typescript', 'd3.js', 'chart.js',
        'three.js', 'socket.io', 'express', 'mongoose', 'sequelize', 'prisma',
        'tensorflow', 'pytorch', 'numpy', 'pandas', 'scipy', 'matplotlib',
        'requests', 'flask', 'django', 'fastapi', 'sqlalchemy', 'celery'
    ]
    
    filename_lower = filename.lower()
    
    # Check if filename contains library patterns
    if any(lib in filename_lower for lib in library_patterns):
        return True
    
    # Check for version numbers in filename (suggests library files)
    version_patterns = [
        r'v\d+\.\d+',           # v1.2, v10.1
        r'_v\d+\.\d+',          # _v1.2
        r'-v\d+\.\d+',          # -v1.2
        r'\d+\.\d+\.\d+',       # 1.2.3
        r'_\d+\.\d+\.\d+',      # _1.2.3
        r'-\d+\.\d+\.\d+',      # -1.2.3
        r'\.min\.',             # minified files
        r'\.bundle\.',          # bundled files
    ]
    
    if any(re.search(pattern, filename_lower) for pattern in version_patterns):
        return True
    
    # Check for specific file types that are typically libraries or unnecessary
    unnecessary_extensions = [
        '.min.js', '.min.css', '.bundle.js', '.bundle.css',
        '.map', '.min.map', '.bundle.map'
    ]
    
    if any(filename_lower.endswith(ext) for ext in unnecessary_extensions):
        return True
    
    # Check if file is in a path that suggests it's a library
    path_parts = file_path.lower().split(os.sep)
    library_path_indicators = [
        'lib', 'libs', 'library', 'libraries', 'vendor', 'vendors',
        'third-party', 'third_party', 'external', 'dependencies',
        'modules', 'packages', 'assets', 'static', 'public',
        'dist', 'build', 'compiled', 'generated'
    ]
    
    if any(indicator in path_parts for indicator in library_path_indicators):
        return True
    
    return False

def is_file_too_long(file_path, max_lines=500, max_size_mb=1):
    """
    Check if a file is too long and likely contains generated or library content.
    Returns True if file should be excluded due to length or size.
    """
    try:
        # Check file size first (faster)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > max_size_mb:
            print(f"[DEBUG] Skipping large file ({file_size_mb:.1f}MB): {file_path}")
            return True
        
        # Then check line count
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            line_count = sum(1 for _ in f)
            
        # Skip very long files that are likely generated or library code
        if line_count > max_lines:
            # Allow some exceptions for our own code
            filename = os.path.basename(file_path).lower()
            
            # Don't exclude our main project files even if long
            if any(keyword in filename for keyword in ['config', 'settings', 'main', 'app', 'index']):
                return False
                
            print(f"[DEBUG] Skipping long file ({line_count} lines): {file_path}")
            return True
            
    except Exception:
        pass  # If we can't read it, let other checks handle it
        
    return False

def is_output_json_file(file_path, filename):
    """
    Determines if a JSON file is likely an output/generated file based on path and naming patterns.
    Returns True if the JSON file should be excluded.
    """
    if not filename.lower().endswith('.json'):
        return False
    
    # Skip JSON files in output/generated directories
    path_parts = file_path.lower().split(os.sep)
    output_indicators = [
        'output', 'outputs', 'results', 'processed', 'generated', 
        'extracted', 'cache', 'temp', 'tmp', 'backup', 'export',
        'reports', 'logs', 'artifacts', 'data', 'json'
    ]
    
    # Check if file is in a directory that suggests it's output
    if any(indicator in path_parts for indicator in output_indicators):
        return True
    
    # Check filename patterns that suggest output files
    filename_lower = filename.lower()
    output_patterns = [
        '_processed.json', '_extracted.json', '_output.json', '_results.json',
        '_cache.json', '_temp.json', '_backup.json', '_export.json',
        '_response.json', '_data.json', '_metadata.json'
    ]
    
    if any(filename_lower.endswith(pattern) for pattern in output_patterns):
        return True
    
    # Check for timestamp patterns in filename (suggests generated files)
    timestamp_patterns = [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{8}',              # YYYYMMDD
        r'\d{4}\d{2}\d{2}_\d{6}',  # YYYYMMDD_HHMMSS
        r'_\d{13}\.json$',     # Unix timestamp
    ]
    
    if any(re.search(pattern, filename_lower) for pattern in timestamp_patterns):
        return True
    
    return False

def get_comment_style(filename):
    """Gets the appropriate comment style based on file extension."""
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    
    # JavaScript family - uses //
    if ext in ['.js', '.jsx', '.ts', '.tsx']:
        return ('// ', '') 
        
    # CSS uses /* ... */ block comments
    elif ext in ['.css']:
        return ('/* ', ' */')
        
    # Python, Shell, YAML, etc. - uses #
    elif ext in ['.py', '.sh', '.yaml', '.yml', '.toml', '.gitignore', '.r', '.pl', '.rb']:
        return ('# ', '')
        
    # HTML family - uses <!-- ... -->
    elif ext in ['.html', '.xml', '.vue', '.svg']:
        return ('<!-- ', ' -->')
        
    # SQL - uses --
    elif ext == '.sql':
        return ('-- ', '')
        
    # Markdown - can use HTML comments
    elif ext == '.md':
        return ('<!-- ', ' -->')
        
    # Special files
    elif filename.lower() == 'requirements.txt':
        return ('# ', '')
        
    # JSON doesn't support comments
    elif ext == '.json':
        return None
        
    # Default for unknown types
    else:
        print(f"[WARN] Unknown file type '{ext}' for header comment. Using '# '.")
        return ('# ', '')

def matches_excluded_pattern(filename):
    """
    Check if filename matches any of the excluded file patterns (including wildcards).
    """
    # Files with wildcard patterns that need special handling
    wildcard_patterns = [
        '*.pyc', '*.pyo', '*.pyd', '*.so', '*.dll', '*.dylib', '*.o', '*.obj',
        '*.exe', '*.out', '*.class', '*.jar', '*.war', '*.swp', '*.swo', '*~',
        '*.tmp', '*.log', 'npm-debug.log*', 'yarn-debug.log*', 'yarn-error.log*',
        'lerna-debug.log*', '*.cover', '*.py,cover'
    ]
    
    return any(fnmatch.fnmatch(filename.lower(), pattern) for pattern in wildcard_patterns)

def should_process_file(file_path, filename):
    """Checks if a file should be processed based on exclusions and allowed types."""
    # Check if path contains node_modules or virtual environment
    if is_venv_or_node_modules(file_path):
        print(f"[DEBUG] Skipping file in node_modules or venv: {file_path}")
        return False
    
    # Include essential documentation files regardless of other exclusions
    relative_path = os.path.relpath(file_path, os.getcwd()).replace('\\', '/')
    if any(relative_path == doc_path or relative_path.endswith(doc_path) for doc_path in ESSENTIAL_DOCS):
        return True
    
    # Check absolute exclusions first
    if filename in EXCLUDED_FILES:
        # print(f"[DEBUG] Skipping explicitly excluded file: {filename}")
        return False
    
    # Check wildcard pattern exclusions
    if matches_excluded_pattern(filename):
        print(f"[DEBUG] Skipping file matching excluded pattern: {filename}")
        return False
    
    # Check for library files and unnecessary content
    if is_library_or_unnecessary_file(file_path, filename):
        print(f"[DEBUG] Skipping library/unnecessary file: {filename}")
        return False
    
    # Check for output JSON files
    if is_output_json_file(file_path, filename):
        print(f"[DEBUG] Skipping output JSON file: {filename}")
        return False
    
    # Check if file is too long (likely generated/library content)
    if is_file_too_long(file_path):
        return False
        
    # Check if it's an allowed specific filename
    if filename in ALLOWED_FILENAMES:
        return True
        
    # Check if it has an allowed extension
    _, ext = os.path.splitext(filename)
    if ext.lower() in ALLOWED_EXTENSIONS:
        return True
        
    # print(f"[DEBUG] Skipping file with disallowed type or name: {filename}")
    return False

def create_file_header(file_path, relative_path):
    """
    Creates a properly formatted header for the file based on its type.
    Returns the header text using the appropriate comment style.
    """
    filename = os.path.basename(file_path)
    comment_style = get_comment_style(filename)
    
    if comment_style is None:  # No comments supported (e.g., JSON)
        return None
    
    comment_start, comment_end = comment_style
    header_content = f"File: {relative_path}"
    
    # For multi-line block comments (CSS, HTML, etc.)
    if comment_end:
        header = f"{comment_start}\n{header_content}\n{comment_end}"
    else:  # Line comments (JS, Python, etc.)
        header = f"{comment_start}{header_content}"
    
    return header

def check_for_existing_header(content, relative_path):
    """
    Checks if the file already has a header about its path.
    Returns the content with ALL existing headers removed.
    """
    # Common header patterns with capture groups
    header_patterns = [
        r'^\s*(//\s*File:.*?)\n',        # JavaScript style
        r'^\s*(#\s*File:.*?)\n',         # Python style
        r'^\s*(/\*\s*File:.*?\*/)',       # CSS style
        r'^\s*(<!--\s*File:.*?-->)',      # HTML style
        r'^\s*(--\s*File:.*?)\n',        # SQL style
    ]
    
    # Check for and remove any header pattern at the beginning of the file
    clean_content = content
    
    # First, try looking for headers at the very beginning
    for pattern in header_patterns:
        clean_content = re.sub(f'^{pattern}\\s*', '', clean_content, flags=re.MULTILINE|re.DOTALL)
    
    # Look for multiple header blocks with separating lines
    clean_content = re.sub(r'^#{80}\s*\n^#\s*File:.*?\n^#{80}\s*\n\s*', '', clean_content, flags=re.MULTILINE|re.DOTALL)
    
    # Check if we have the file path in a header anywhere in the first 10 lines
    first_lines = content.split('\n')[:10]
    first_block = '\n'.join(first_lines)
    
    file_path_pattern = re.escape(relative_path)
    has_header = re.search(file_path_pattern, first_block) is not None
    
    return has_header, clean_content

def prepend_header_if_needed(content, header, relative_path):
    """
    Prepends a header to the content if no suitable header exists.
    Returns the content with a header.
    """
    if header is None:
        return content
    
    # Check if content already has a header and clean up any duplicates
    has_header, clean_content = check_for_existing_header(content, relative_path)
    
    # If it already has a header, just return the cleaned content
    if has_header:
        return clean_content
    
    # Add the header to the cleaned content
    return f"{header}\n\n{clean_content}"

def generate_directory_structure(root_dir='.'):
    """Generates a comprehensive text representation of the directory structure with file details."""
    print("[DEBUG] Generating directory structure...")
    structure = ["# Directory Structure", "#" * 80]
    processed_paths = set() 
    abs_root = os.path.abspath(root_dir)
    abs_excluded_dirs = {os.path.join(abs_root, d) for d in EXCLUDED_DIRS}

    def format_file_size(size_bytes):
        """Convert bytes to human readable format."""
        if size_bytes == 0:
            return "0B"
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        return f"{size_bytes:.1f}{size_names[i]}"

    def get_file_info(file_path):
        """Get file information including size and type."""
        try:
            size = os.path.getsize(file_path)
            _, ext = os.path.splitext(file_path)
            ext = ext.lower() if ext else 'no ext'
            return f" ({format_file_size(size)}, {ext})"
        except OSError:
            return " (size unknown)"

    def add_directory(path, prefix=""):
        real_path = os.path.realpath(path)
        if real_path in processed_paths:
            structure.append(f"{prefix}[WARN] Symlink loop or duplicate processing: {path}")
            return
        processed_paths.add(real_path)

        # For the root directory, don't check exclusions - we want to show everything
        is_root = (real_path == abs_root)
        
        if not is_root:
            # Additional check for node_modules and virtual environments
            if is_venv_or_node_modules(real_path):
                structure.append(f"{prefix}[EXCLUDED] Virtual environment or node_modules: {os.path.basename(real_path)}/")
                return
                
            # Check if the current directory is in an excluded path
            if is_path_excluded(real_path, abs_root):
                structure.append(f"{prefix}[EXCLUDED] Excluded path: {os.path.basename(real_path)}/")
                return
                
            # Check if the current directory itself is excluded
            if any(os.path.basename(real_path) == d for d in EXCLUDED_DIRS):
                structure.append(f"{prefix}[EXCLUDED] Excluded directory: {os.path.basename(real_path)}/")
                return
        
        # Check if path is *under* an excluded dir (needed for topdown=False or initial call)
        if not is_root:
            is_under_excluded = any(real_path.startswith(excluded + os.path.sep) or real_path == excluded for excluded in abs_excluded_dirs)
            if is_under_excluded:
                return

        try:
            items = sorted(os.listdir(path))
        except OSError as e:
            print(f"[WARN] Could not list directory {path}: {e}")
            structure.append(f"{prefix}[ERROR] Cannot access directory: {e}")
            return

        entries = []
        excluded_items = []
        
        for item in items:
             item_path = os.path.join(path, item)
             item_real_path = os.path.realpath(item_path)
             
             is_dir = os.path.isdir(item_path)
             is_file = os.path.isfile(item_path)

             # Track excluded items for summary
             if is_venv_or_node_modules(item_path):
                 excluded_items.append((item, "venv/node_modules"))
                 continue

             # Check directory exclusions
             if is_dir:
                 if item not in EXCLUDED_DIRS and item_real_path not in abs_excluded_dirs:
                      is_under = any(item_real_path.startswith(excluded + os.path.sep) or item_real_path == excluded for excluded in abs_excluded_dirs)
                      if not is_under:
                          entries.append((item, True, None)) # Mark as directory
                      else:
                          excluded_items.append((item, "excluded dir"))
                 else:
                     excluded_items.append((item, "excluded dir"))
             # Check file exclusions
             elif is_file:
                 if item not in EXCLUDED_FILES:
                     file_info = get_file_info(item_path)
                     entries.append((item, False, file_info)) # Mark as file with info
                 else:
                     excluded_items.append((item, "excluded file"))
        
        # Add included entries
        for i, (entry_name, is_dir_entry, file_info) in enumerate(entries):
            is_last = (i == len(entries) - 1) and len(excluded_items) == 0
            connector = "└── " if is_last else "├── "
            
            if is_dir_entry:
                 structure.append(f"{prefix}{connector}{entry_name}/")
                 child_prefix = prefix + ("    " if is_last else "│   ")
                 add_directory(os.path.join(path, entry_name), child_prefix)
            else:
                 structure.append(f"{prefix}{connector}{entry_name}{file_info}")
        
        # Add summary of excluded items if any
        if excluded_items:
            connector = "└── " if len(entries) == 0 else "├── "
            structure.append(f"{prefix}{connector}[EXCLUDED] {len(excluded_items)} items: {', '.join([f'{name} ({reason})' for name, reason in excluded_items[:3]])}")
            if len(excluded_items) > 3:
                structure.append(f"{prefix}    ... and {len(excluded_items) - 3} more excluded items")

    add_directory(os.path.abspath(root_dir))
    print("[DEBUG] Directory structure generation complete.")
    return "\n".join(structure)


def is_path_excluded(path, root_dir):
    """
    Checks if the given path is in an excluded path.
    """
    rel_path = os.path.relpath(path, root_dir)
    for excluded_path in EXCLUDED_PATHS:
        # Check if rel_path is or starts with the excluded path
        if rel_path == excluded_path or rel_path.startswith(excluded_path + os.sep):
            return True
    return False

def collect_file_contents(root_dir='.'):
    """
    Collects contents of all files to be processed, returning a list of file blocks
    where each block contains the file path and content.
    """
    print(f"[DEBUG] Starting content collection process. Root: {root_dir}")
    abs_root = os.path.abspath(root_dir)
    abs_excluded_dirs = {os.path.join(abs_root, d) for d in EXCLUDED_DIRS}
    
    file_blocks = []
    
    # --- Walk Directory and Process Files ---
    print(f"[DEBUG] Walking directory tree from: {abs_root}")
    processed_files_count = 0
    skipped_files_count = 0
    skipped_venv_count = 0
    skipped_node_modules_count = 0

    for root, dirs, files in os.walk(abs_root, topdown=True):
        # Skip this directory and its subdirectories if it's a virtual env or node_modules
        if is_venv_or_node_modules(root):
            print(f"[DEBUG] Skipping virtual environment or node_modules directory: {root}")
            if 'node_modules' in root:
                skipped_node_modules_count += 1
            else:
                skipped_venv_count += 1
            dirs[:] = []  # Skip all subdirectories
            continue
            
        # Skip if the current directory is in an excluded path
        if is_path_excluded(root, abs_root):
            print(f"[DEBUG] Skipping excluded path directory: {root}")
            dirs[:] = []  # Skip all subdirectories
            continue

        # Filter excluded directories *before* recursion
        dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS and not is_venv_or_node_modules(os.path.join(root, d))]
        
        files.sort()

        relative_root = os.path.relpath(root, abs_root)
        if relative_root == '.': relative_root = '' 

        # Safety check: ensure current root isn't inside an excluded dir
        is_in_excluded_dir = any(root.startswith(excluded + os.path.sep) or root == excluded for excluded in abs_excluded_dirs)
        if is_in_excluded_dir: 
            continue

        for file in files:
            file_path = os.path.join(root, file)
            relative_file_path = os.path.normpath(os.path.join(relative_root, file))

            # 1. Check if file should be processed at all (type, name, exclusion)
            if not should_process_file(file_path, file):
                skipped_files_count += 1
                continue

            # 2. Read content for concatenation
            print(f"[DEBUG] Processing file for concatenation: {relative_file_path}")
            processed_files_count += 1
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read().strip()
                
                # 3. Create and add a properly formatted header
                header = create_file_header(file_path, relative_file_path)
                content_with_header = prepend_header_if_needed(content, header, relative_file_path)
                
                # 4. Create the block for the concatenated output
                block_content = []
                block_content.append("#" * 80)
                block_content.append(f"# File: {relative_file_path}")
                block_content.append("#" * 80 + "\n")
                block_content.append(content_with_header) 
                block_content.append("\n\n" + "="*80 + "\n\n")  # Separator
                
                file_blocks.append({
                    'path': relative_file_path,
                    'content': "\n".join(block_content),
                    'size': len("\n".join(block_content))
                })

            except Exception as e:
                print(f"[WARN] Error reading {file_path} for concatenation: {e}. Skipping content.")
                # Add error note as a block
                block_content = []
                block_content.append("#" * 80)
                block_content.append(f"# File: {relative_file_path}")
                block_content.append("#" * 80 + "\n")
                block_content.append(f"[ERROR: Could not read file content due to: {e}]\n\n")
                block_content.append("="*80 + "\n\n")
                
                file_blocks.append({
                    'path': relative_file_path,
                    'content': "\n".join(block_content),
                    'size': len("\n".join(block_content))
                })

    print(f"[INFO] Successfully processed {processed_files_count} files")
    print(f"[INFO] Skipped {skipped_files_count} files (excluded types/names)")
    print(f"[INFO] Skipped {skipped_venv_count} virtual environment directories")
    print(f"[INFO] Skipped {skipped_node_modules_count} node_modules directories")
    return file_blocks, processed_files_count, skipped_files_count


def distribute_files_across_parts(file_blocks, num_parts=3):
    """
    Distributes file blocks across multiple parts ensuring roughly equal size
    and that no file is split across parts.
    """
    # Calculate total size
    total_size = sum(block['size'] for block in file_blocks)
    target_size_per_part = total_size / num_parts
    
    print(f"[DEBUG] Total content size: {total_size} bytes")
    print(f"[DEBUG] Target size per part: {target_size_per_part} bytes")
    
    # Sort files by size (largest first) to help balance distribution
    file_blocks.sort(key=lambda x: x['size'], reverse=True)
    
    # Initialize parts
    parts = [[] for _ in range(num_parts)]
    part_sizes = [0] * num_parts
    
    # Greedy algorithm to distribute files
    for block in file_blocks:
        # Find the part with the smallest current size
        smallest_part_idx = part_sizes.index(min(part_sizes))
        
        # Add the file to that part
        parts[smallest_part_idx].append(block)
        part_sizes[smallest_part_idx] += block['size']
    
    # Print the distribution results
    for i, size in enumerate(part_sizes):
        print(f"[INFO] Part {i+1} size: {size} bytes ({len(parts[i])} files)")
    
    return parts


def write_parts_to_files(parts, root_dir='.'):
    """Writes each part to a separate file without duplicating content."""
    abs_root = os.path.abspath(root_dir)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Generate directory structure once for part 1 only
    directory_structure = generate_directory_structure(abs_root)
    
    # Create file index showing which files are in which parts
    file_index = ["# File Index - Which Files Are in Which Parts", "#" * 80]
    for i, part in enumerate(parts, 1):
        file_index.append(f"\n## Part {i} ({len(part)} files):")
        for block in part:
            file_index.append(f"  - {block['path']}")
    file_index_content = "\n".join(file_index)
    
    for i, part in enumerate(parts, 1):
        output_file = OUTPUT_FILENAME_TEMPLATE.format(i)
        output_path = os.path.join(abs_root, output_file)
        
        # Create content
        all_content = []
        
        # Add header
        concatenated_header = (
            f"# Concatenated Project Code - Part {i} of {len(parts)}\n"
            f"# Generated: {timestamp}\n"
            f"# Root Directory: {abs_root}\n"
            f"{'='*80}\n"
        )
        all_content.append(concatenated_header)
        
        # Add directory structure only to part 1
        if i == 1:
            all_content.append(directory_structure)
            all_content.append("\n\n" + "="*80 + "\n\n")
        
        # Add file index to all parts for navigation
        all_content.append(file_index_content)
        all_content.append("\n\n" + "="*80 + "\n\n")
        
        # Add file contents for this part
        for block in part:
            all_content.append(block['content'])
        
        # Write the file
        print(f"[DEBUG] Writing part {i} to: {output_path}")
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(all_content))
            print(f"[INFO] Successfully created {output_path} with {len(part)} files")
        except Exception as e:
            print(f"[ERROR] Critical error writing output file {output_path}: {e}")


# --- Main Function ---
def split_concatenated_scripts(num_parts=3, root_dir='.'):
    """
    Collects file contents, splits them into multiple parts with similar sizes,
    and writes each part to a separate file.
    """
    # 1. Collect all file contents
    file_blocks, processed_count, skipped_count = collect_file_contents(root_dir)
    
    # 2. Distribute files across parts
    parts = distribute_files_across_parts(file_blocks, num_parts)
    
    # 3. Write each part to a file
    write_parts_to_files(parts, root_dir)
    
    print(f"[INFO] Successfully split {processed_count} files into {num_parts} parts")
    print(f"[INFO] Files created: {', '.join([OUTPUT_FILENAME_TEMPLATE.format(i+1) for i in range(num_parts)])}")


# --- Main Execution ---
if __name__ == '__main__':
    split_concatenated_scripts(num_parts=3, root_dir='.') 