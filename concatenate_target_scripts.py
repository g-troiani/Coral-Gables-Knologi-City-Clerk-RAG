import os
import sys
import datetime
import re
import fnmatch
import argparse

# --- Configuration Constants ---

# Default targets if none specified - PART 3: Graph RAG Stages Phase 3
DEFAULT_TARGETS = [
    './scripts/graph_rag_stages/phase3_querying',
    './scripts/graph_rag_stages/common',
    './ui',
    './simple_graph_viewer.py',
    './interactive_graph_viewer.py',
    './single_meeting_graph_viewer.py'
]

# Output filename
OUTPUT_FILENAME = 'concatenated_target_scripts.txt'

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
SCRIPT_FILENAME = os.path.basename(sys.argv[0]) 

EXCLUDED_FILES = [
    'concatenated_scripts_part1.txt',
    'concatenated_scripts_part2.txt',
    'concatenated_scripts_part3.txt',
    'concatenated_target_scripts.txt',  # Exclude our own output
    SCRIPT_FILENAME, # Exclude the script file itself
    '.env', # Exclude environment variable files
    '.DS_Store', # macOS system file
    'city_clerk_graph.html', # Exclude specific HTML file
    # RAG Pipeline Files - Exclude entire RAG system
    'rag_local_web_app.py',
    'pipeline_modular_optimized.py',
    'simple_graph_viewer.py',
    'delete_all_data.py',
    'supabase_clear_database.py',
    'test_vector_search.py',
    'find_duplicates.py',
    'topic_filter_and_title.py',
    # GraphRAG output and data files
    'city_clerk_documents.csv',
    'graphrag_run.log',
    'live_monitor.py',
    'test_query.py',
    'analyze_docs.py',
    # GraphRAG specific output files
    'indexing-engine.log',
    'entities.parquet',
    'relationships.parquet',
    'communities.parquet',
    'community_reports.parquet',
    'text_units.parquet',
    'documents.parquet',
    'create_base_extracted_entities.parquet',
    'create_base_entity_graph.parquet',
    'create_final_entities.parquet',
    'create_final_relationships.parquet',
    'create_final_communities.parquet',
    'create_final_community_reports.parquet',
    'domain_examples.txt',
    'entity_extraction.txt',
    'community_report.txt',
    'summarize_descriptions.txt',
    # Pipeline output files
    'pipeline_results.json',
    'extraction_results.json',
    'processing_log.txt',
    'pipeline_log.txt',
    'pipeline.log',
    'ontology_model.txt',
    'ontology_modelv2.txt',
    'monitor_log.txt',
    'extraction_log.txt',
    'processing_summary.json',
    'extraction_summary.json',
    'pipeline_status.json',
    'run_summary.json',
    'performance_metrics.json',
    # Graph database output files
    'graph_analysis.json',
    'network_analysis.json',
    'node_analysis.json',
    'edge_analysis.json',
    'community_detection.json',
    'centrality_analysis.json',
    'graph_metrics.json',
    'graph_export.gexf',
    'graph_export.graphml',
    'graph_export.gml',
    'network_export.json',
    'adjacency_matrix.csv',
    'edge_list.csv',
    'node_list.csv',
    'graph_visualization.html',
    'network_visualization.html',
    # Token counting and analysis files
    'token_counts.json',
    'token_analysis.json',
    'content_analysis.json',
    'document_stats.json',
    'processing_stats.json',
    # Test and debug files
    'test_python_detection.py',
    'test_integration.py',
    'debug_output.txt',
    'test_output.json',
    'debug_log.txt',
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
    # Processed chunk index files
    'chunk_index.json',
    'processing_audit.json',
    # Extracted JSON files from pipeline stages
    '*_stage1_ocr.json',
    '*_stage2_agenda.json', 
    '*_stage3_ontology.json',
    '*_verbatim_transcript.json',
    '*_enhanced_ordinance.json',
    '*_enhanced_resolution.json',
            # Removed: '*_enhanced_legal_documents.json' (aggregated files no longer created)
    '*_verbatim_transcript_collection.json',
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
    'city_clerk_documents/extracted_text',     # Pipeline extracted text output
    'city_clerk_documents/extracted_markdown', # Pipeline markdown output
    'city_clerk_documents/processed',          # Any processed documents
    'city_clerk_documents/cache',              # Document processing cache
    'city_clerk_documents/graph_json',         # Processed JSON outputs from documents
    'city_clerk_documents/global copy',        # Copy of source documents directory
    'city_clerk_documents/global copy 2',      # Another copy of source documents directory
    'city_clerk_documents/extracted_json',     # Extracted JSON files
    'documents/',
    'debug',              # Document processing debug outputs
    'prompts',            # Generated prompts from document processing
    # Processed chunks directories - EXCLUDE ALL PROCESSED CHUNKS
    'simple_ner_graph',              # Entire simple NER graph directory with processed chunks
    'simple_ner_graph/document_chunks', # Processed document chunks directory  
    'simple_ner_graph/entities',        # Processed entities directory
    'local_graph_data',              # Local graph data with processed content
    'local_graph_data/document_chunks', # Local graph processed chunks
    'document_chunks',               # Any document_chunks directory
    # GraphRAG Directories - Exclude GraphRAG processing directories  
    'graphrag_data',          # Entire GraphRAG working directory
    'graphrag_data/output',   # GraphRAG output files
    'graphrag_data/logs',     # GraphRAG processing logs
    'graphrag_data/cache',    # GraphRAG cache files
    'graphrag_data/artifacts', # GraphRAG artifacts
    'graphrag_data/prompts',  # Generated GraphRAG prompts
    'graphrag_data/input',    # GraphRAG input processing
    'graphrag_data/storage',  # GraphRAG storage
    # RAG Pipeline Directories - Only exclude old RAG_stages, keep graph_rag_stages for part 3
    'RAG_stages',         # RAG pipeline stages directory (old version)
    'scripts/RAG_stages', # RAG stages in scripts directory (old version)
    'scripts/graph_rag_stages/phase1_preprocessing',  # Exclude phase1 for part 3
    'scripts/graph_rag_stages/phase2_building',       # Exclude phase2 for part 3
    'pipeline_output',    # General pipeline output
    'processing_output',  # Processing output directory
    'extracted_output',   # Extraction output directory
    'vectorstore',        # Vector database storage
    'embeddings',         # Embeddings cache/storage
    'index',              # Search index files
    'search_index',       # Search index files
    'vector_index',       # Vector index files
    'chroma_db',          # ChromaDB storage
    'faiss_index',        # FAISS index storage
    'lancedb',            # LanceDB storage
    'qdrant_storage',     # Qdrant storage
    # Output directories from graph_database pipeline
    'output',             # General output directory
    'results',            # Results directory
    'processed_data',     # Processed data output
    'analysis_results',   # Analysis results
    'graph_output',       # Graph analysis output
    'network_output',     # Network analysis output
    'visualization_output', # Visualization files
    'exports',            # Export directories
    'backups',            # Backup directories
    # Extracted JSON directories from pipeline stages
    'city_clerk_documents/extracted_json',     # Primary extracted JSON output directory
    'test_verbatim_json', # Test extracted JSON files
    # Library and vendor directories
    'lib',                # Library directories
    'libs',               # Library directories
    'files',              # Files directory
    'ontologymodels',     # Ontology models directory
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

# Additional patterns to identify virtual environments
VENV_PATTERNS = [
    'venv', 'virtualenv', 'env', 'python3', 'python', 'city_clerk_rag', 'city-clerk-rag',
    '.venv', '.env', 'venv_', 'env_'  # Additional common virtual environment patterns
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

def is_file_too_long(file_path, max_lines=2000, max_size_mb=2):
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
        'lerna-debug.log*', '*.cover', '*.py,cover',
        # Data and output files
        '*.csv', '*.parquet', '*.db', '*.sqlite', '*.sqlite3',
        # GraphRAG specific files
        'graphrag_*.log', '*_monitor_*.log', '*.lancedb',
        # Pipeline output files with timestamps or dynamic names
        '*_extracted.json', '*_processed.json', '*_results.json',
        '*_output.json', '*_summary.json', '*_report.json',
        '*_analysis.json', '*_metrics.json', '*_stats.json',
        'pipeline_*', 'extraction_*', 'processing_*',
        'graph_*', 'network_*', 'community_*',
        # GraphRAG workflow files
        'create_*.parquet', 'final_*.parquet', 'base_*.parquet',
        # Log files from pipelines
        '*_pipeline.log', '*_extraction.log', '*_processing.log',
        '*_indexing.log', '*_graph.log', '*_monitor.log',
        # Backup and temporary files
        '*.backup', '*.bak', '*.temp', '*.cache',
        # Export files
        '*.gexf', '*.graphml', '*.gml', '*.gephi',
        # Vector database files
        '*.faiss', '*.ann', '*.hnsw', '*.ivf',
        # Archive and compressed files that are likely outputs
        '*_output.zip', '*_results.tar.gz', '*_export.zip',
        # Test and debug files
        'test_*.py', 'debug_*', '*_test.json', '*_debug.log',
        # Extracted JSON files from pipeline stages
        '*_stage1_ocr.json', '*_stage2_agenda.json', '*_stage3_ontology.json',
        '*_verbatim_transcript.json', '*_enhanced_*.json', '*_collection.json'
    ]
    
    return any(fnmatch.fnmatch(filename.lower(), pattern) for pattern in wildcard_patterns)

def should_process_file(file_path, filename):
    """Checks if a file should be processed based on exclusions and allowed types."""
    # Check if path contains node_modules or virtual environment
    if is_venv_or_node_modules(file_path):
        print(f"[DEBUG] Skipping file in node_modules or venv: {file_path}")
        return False
    
    # Check absolute exclusions first
    if filename in EXCLUDED_FILES:
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

def is_directory_excluded(dir_path, root_dir):
    """
    Checks if a directory should be excluded based on both simple directory names 
    and path-based exclusions.
    """
    # Get the directory name
    dir_name = os.path.basename(dir_path)
    
    # Check if the directory name itself is excluded
    if dir_name in EXCLUDED_DIRS:
        return True
    
    # Get the relative path from root
    rel_path = os.path.relpath(dir_path, root_dir).replace('\\', '/')
    
    # Check path-based exclusions
    for excluded_dir in EXCLUDED_DIRS:
        # If excluded_dir contains a path separator, treat it as a path-based exclusion
        if '/' in excluded_dir:
            if rel_path == excluded_dir or rel_path.startswith(excluded_dir + '/'):
                return True
        # Also handle Windows-style paths
        elif '\\' in excluded_dir:
            excluded_dir_normalized = excluded_dir.replace('\\', '/')
            if rel_path == excluded_dir_normalized or rel_path.startswith(excluded_dir_normalized + '/'):
                return True
    
    return False

def process_single_file(file_path, base_dir=None):
    """
    Process a single file and return its content block.
    """
    if not os.path.exists(file_path):
        print(f"[WARN] File does not exist: {file_path}")
        return None
    
    if not os.path.isfile(file_path):
        print(f"[WARN] Path is not a file: {file_path}")
        return None
    
    filename = os.path.basename(file_path)
    
    # Check if file should be processed
    if not should_process_file(file_path, filename):
        return None
    
    # Determine relative path
    if base_dir:
        relative_path = os.path.relpath(file_path, base_dir)
    else:
        relative_path = filename
    
    print(f"[DEBUG] Processing file: {relative_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read().strip()
        
        # Create and add a properly formatted header
        header = create_file_header(file_path, relative_path)
        content_with_header = prepend_header_if_needed(content, header, relative_path)
        
        # Create the block for the concatenated output
        block_content = []
        block_content.append("#" * 80)
        block_content.append(f"# File: {relative_path}")
        block_content.append("#" * 80 + "\n")
        block_content.append(content_with_header) 
        block_content.append("\n\n" + "="*80 + "\n\n")  # Separator
        
        return {
            'path': relative_path,
            'content': "\n".join(block_content)
        }
        
    except Exception as e:
        print(f"[WARN] Error reading {file_path}: {e}. Skipping content.")
        # Add error note as a block
        block_content = []
        block_content.append("#" * 80)
        block_content.append(f"# File: {relative_path}")
        block_content.append("#" * 80 + "\n")
        block_content.append(f"[ERROR: Could not read file content due to: {e}]\n\n")
        block_content.append("="*80 + "\n\n")
        
        return {
            'path': relative_path,
            'content': "\n".join(block_content)
        }

def process_directory(dir_path, base_dir=None):
    """
    Process all files in a directory and its subdirectories.
    Returns a list of file blocks.
    """
    if not os.path.exists(dir_path):
        print(f"[WARN] Directory does not exist: {dir_path}")
        return []
    
    if not os.path.isdir(dir_path):
        print(f"[WARN] Path is not a directory: {dir_path}")
        return []
    
    abs_dir = os.path.abspath(dir_path)
    if base_dir is None:
        base_dir = abs_dir
    
    file_blocks = []
    
    for root, dirs, files in os.walk(abs_dir, topdown=True):
        # Skip virtual environments and node_modules
        if is_venv_or_node_modules(root):
            print(f"[DEBUG] Skipping virtual environment or node_modules directory: {root}")
            dirs[:] = []  # Skip all subdirectories
            continue
            
        # Skip excluded paths
        if is_path_excluded(root, base_dir):
            print(f"[DEBUG] Skipping excluded path directory: {root}")
            dirs[:] = []  # Skip all subdirectories
            continue

        # Filter excluded directories
        dirs[:] = [d for d in dirs if not is_directory_excluded(os.path.join(root, d), base_dir) 
                   and not is_venv_or_node_modules(os.path.join(root, d))]
        
        files.sort()

        for file in files:
            file_path = os.path.join(root, file)
            block = process_single_file(file_path, base_dir)
            if block:
                file_blocks.append(block)
    
    return file_blocks

def collect_from_targets(targets):
    """
    Collect files from all specified targets (files and/or directories).
    """
    all_file_blocks = []
    processed_paths = set()  # Track processed files to avoid duplicates
    
    # Determine the common base directory for all targets
    abs_targets = [os.path.abspath(t) for t in targets]
    common_base = os.path.commonpath(abs_targets) if len(abs_targets) > 1 else os.path.dirname(abs_targets[0])
    
    print(f"\n[INFO] Processing {len(targets)} target(s)")
    print(f"[INFO] Common base directory: {common_base}")
    
    for target in targets:
        abs_target = os.path.abspath(target)
        
        if os.path.isfile(abs_target):
            # Process single file
            if abs_target not in processed_paths:
                print(f"\n[INFO] Processing file: {target}")
                block = process_single_file(abs_target, common_base)
                if block:
                    all_file_blocks.append(block)
                    processed_paths.add(abs_target)
            else:
                print(f"[INFO] Skipping duplicate file: {target}")
                
        elif os.path.isdir(abs_target):
            # Process directory
            print(f"\n[INFO] Processing directory: {target}")
            dir_blocks = process_directory(abs_target, common_base)
            
            # Add only non-duplicate files
            for block in dir_blocks:
                file_path = os.path.join(common_base, block['path'])
                abs_file_path = os.path.abspath(file_path)
                if abs_file_path not in processed_paths:
                    all_file_blocks.append(block)
                    processed_paths.add(abs_file_path)
                    
        else:
            print(f"[WARN] Target does not exist: {target}")
    
    return all_file_blocks, len(processed_paths)

def write_concatenated_file(file_blocks, targets, output_file):
    """Writes all file blocks to the output file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create content
    all_content = []
    
    # Add header
    targets_str = ", ".join(targets)
    concatenated_header = (
        f"# Concatenated Scripts - Graph RAG Stages Part 3 (Phase 3 Querying)\n"
        f"# Generated: {timestamp}\n"
        f"# Targets: {targets_str}\n"
        f"# Total Files: {len(file_blocks)}\n"
        f"# Focus: Query processing, response generation, and graph interaction\n"
        f"{'='*80}\n\n"
    )
    all_content.append(concatenated_header)
    
    # Add file index
    file_index = ["# File Index", "#" * 80]
    for i, block in enumerate(file_blocks, 1):
        file_index.append(f"{i:3d}. {block['path']}")
    file_index.append("\n" + "="*80 + "\n")
    all_content.append("\n".join(file_index))
    
    # Add file contents
    for block in file_blocks:
        all_content.append(block['content'])
    
    # Write the file
    output_path = os.path.abspath(output_file)
    print(f"\n[INFO] Writing concatenated file to: {output_path}")
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(all_content))
        print(f"[INFO] Successfully created {output_path}")
        
        # Calculate and display file size
        file_size = os.path.getsize(output_path) / (1024 * 1024)
        print(f"[INFO] Output file size: {file_size:.2f} MB")
        
    except Exception as e:
        print(f"[ERROR] Critical error writing output file {output_path}: {e}")
        sys.exit(1)

# --- Main Function ---
def main():
    """Main function to handle command line arguments and run concatenation for graph_rag_stages part 3."""
    parser = argparse.ArgumentParser(
        description='Concatenate Graph RAG Stages Part 3 (Phase 3 Querying) files into a single file.',
        epilog='Examples:\n'
               '  %(prog)s                        # Default: concatenate phase3_querying components\n'
               '  %(prog)s script1.py script2.js  # Concatenate specific files\n'
               '  %(prog)s ./phase3_querying      # Concatenate specific directory\n',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'targets',
        nargs='*',
        help='Files and/or directories to concatenate'
    )
    parser.add_argument(
        '-o', '--output',
        default=OUTPUT_FILENAME,
        help=f'Output filename (default: {OUTPUT_FILENAME})'
    )
    
    args = parser.parse_args()
    
    # Use default targets if none specified
    targets = args.targets if args.targets else DEFAULT_TARGETS
    
    # Collect all file contents from targets
    file_blocks, total_files = collect_from_targets(targets)
    
    if not file_blocks:
        print(f"[WARN] No files found to concatenate from targets: {', '.join(targets)}")
        sys.exit(0)
    
    # Write to output file
    write_concatenated_file(file_blocks, targets, args.output)
    
    print(f"\n[SUCCESS] Concatenation complete!")
    print(f"  - Targets processed: {len(targets)}")
    print(f"  - Files concatenated: {total_files}")
    print(f"  - Output file: {args.output}")

# --- Main Execution ---
if __name__ == '__main__':
    main()