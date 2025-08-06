#!/usr/bin/env python3
################################################################################
# File: rag_web_interface.py          (Supabase / pgvector demo – v2025‑05‑06)
################################################################################
"""
Mini Flask app that answers questions with Graph Retrieval‑Augmented
Generation (gpt-4.1-mini-2025-04-14 + Supabase pgvector).

### Patch 2  (2025‑05‑06)
• **Embeddings** now created with **text‑embedding‑ada‑002** (1536‑D).  
• Similarity is re‑computed client‑side with a **plain cosine function** so the
  ranking no longer depends on pgvector's built‑in distance or any RPC
  threshold quirks.

The rest of the grounded‑answer logic (added in Patch 1) is unchanged.
"""


from __future__ import annotations

import logging
import math
import os
import re
import sys
from pathlib import Path
from typing import Dict, List
import json

from dotenv import load_dotenv
from flask import Flask, jsonify, request, make_response
from openai import OpenAI
from openai import AzureOpenAI
from supabase import create_client
from flask_compress import Compress
from flask_cors import CORS

# Import Simple NER functionality
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.append(str(project_root))

try:
    from scripts.graph_rag_stages.phase3_querying.ner import UnifiedQueryEngine
    SIMPLE_NER_AVAILABLE = True
except ImportError:
    SIMPLE_NER_AVAILABLE = False

# ────────────────────────────── configuration ───────────────────────────── #

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
PORT           = int(os.getenv("PORT", 8080))

if not (OPENAI_API_KEY and SUPABASE_URL and SUPABASE_KEY):
    raise SystemExit(
        "❌  Required env vars: OPENAI_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY"
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)
log = logging.getLogger("rag_app")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
oa = OpenAI(api_key=OPENAI_API_KEY)  # For embeddings
# Initialize Azure OpenAI client for chat completions
azure_client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").split(" #")[0].strip().strip('"')
)

app = Flask(__name__)
CORS(app)
app.config['COMPRESS_ALGORITHM'] = 'gzip'
Compress(app)

# Initialize UnifiedQueryEngine if available
unified_query_engine = None
if SIMPLE_NER_AVAILABLE:
    try:
        SIMPLE_NER_ROOT = project_root / "simple_ner_graph"
        unified_query_engine = UnifiedQueryEngine(SIMPLE_NER_ROOT)
        log.info("✅ UnifiedQueryEngine initialized")
    except Exception as e:
        log.warning(f"⚠️  UnifiedQueryEngine failed to initialize: {e}")
        SIMPLE_NER_AVAILABLE = False

# ────────────────────────────── helper functions ────────────────────────── #


def embed(text: str) -> List[float]:
    """
    Return OpenAI embedding vector for *text* using text‑embedding‑ada‑002.

    ada‑002 has 1536 dimensions and is inexpensive yet solid for similarity.
    """
    resp = oa.embeddings.create(
        model="text-embedding-ada-002",
        input=text[:8192],  # safety slice
    )
    return resp.data[0].embedding


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Plain cosine similarity between two equal‑length vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb + 1e-9)


# Add in-memory embedding cache
_qcache = {}
def embed_cached(text):
    if text in _qcache: return _qcache[text]
    vec = embed(text)
    _qcache[text] = vec
    return vec


# Add regex patterns for bibliography detection
_DOI_RE   = re.compile(r'\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b', re.I)
_YEAR_RE  = re.compile(r'\b(19|20)\d{2}\b')

def looks_like_refs(text: str) -> bool:
    """
    Return True if this chunk is likely just a bibliography list:
      • more than 12 DOIs, or
      • more than 15 year mentions.
    """
    doi_count  = len(_DOI_RE.findall(text))
    year_count = len(_YEAR_RE.findall(text))
    return doi_count > 12 or year_count > 15








# ──────────────────────── NEW RAG‑PROMPT HELPERS ───────────────────────── #

MAX_PROMPT_CHARS: int = 24_000  # ~6 k tokens @ 4 chars/token heuristic


def trim_chunks(chunks: List[Dict]) -> List[Dict]:
    """
    Fail‑safe guard: ensure concatenated chunk texts remain under the
    MAX_PROMPT_CHARS budget.  Keeps highest‑similarity chunks first.
    """
    sorted_chunks = sorted(chunks, key=lambda c: c.get("similarity", 0), reverse=True)
    output: List[Dict] = []
    total_chars = 0
    for c in sorted_chunks:
        chunk_len = len(c["text"])
        if total_chars + chunk_len > MAX_PROMPT_CHARS:
            break
        output.append(c)
        total_chars += chunk_len
    return output


def build_prompt(question: str, chunks: List[Dict]) -> str:
    """
    Build a structured prompt that asks GPT to:
      • answer in Markdown with short intro + numbered list of key points
      • cite inline like [1], [2] …
      • finish with a Bibliography that includes the document title and type
    """
    snippet_lines, biblio_lines = [], []
    for i, c in enumerate(chunks, 1):
        page_start = c.get('page_start', 1)
        page_end = c.get('page_end', 1)
        snippet_lines.append(
            f"[{i}] \"{c['text'].strip()}\" "
            f"(pp. {page_start}-{page_end})"
        )

        d = c["doc"]
        title = d.get("title", "Untitled Document")
        doc_type = d.get("document_type", "Document")
        date = d.get("date", "Unknown date")
        year = d.get("year", "n.d.")
        pages = f"pp. {page_start}-{page_end}"
        source_pdf = d.get("source_pdf", "")

        # City clerk document bibliography format
        biblio_lines.append(
            f"[{i}] *{title}* · {doc_type} · {date} · {pages}"
        )

    prompt_parts = [
        "You are City Clerk Assistant, a knowledgeable AI that helps with questions about city government documents, including resolutions, ordinances, proclamations, contracts, meeting minutes, and agendas.",
        "You draw on evidence from official city documents and municipal records.",
        "Your responses are clear, professional, and grounded in the provided context.",
        "====",
        "QUESTION:",
        question,
        "====",
        "CONTEXT:",
        *snippet_lines,
        "====",
        "INSTRUCTIONS:",
        "• Write your answer in **Markdown**.",
        "• Begin with a concise summary (2–3 sentences).",
        "• Then elaborate on key points using well-structured paragraphs.",
        "• Provide relevant insights about city governance, policies, or procedures.",
        "• If helpful, use lists, subheadings, or clear explanations to enhance understanding.",
        "• Use a professional and informative tone.",
        "• Cite sources inline like [1], [2] etc.",
        "• After the answer, include a 'BIBLIOGRAPHY:' section that lists each source exactly as provided below.",
        "• If none of the context answers the question, reply: \"I'm sorry, I don't have sufficient information to answer that.\"",
        "====",
        "BEGIN OUTPUT",
        "ANSWER:",
        "",  # where the model writes the main response
        "BIBLIOGRAPHY:",
        *biblio_lines,
    ]

    return '\n'.join(prompt_parts)


def extract_citations(answer: str) -> List[str]:
    """
    Parse numeric citations (e.g., "[1]", "[2]") from the answer text.
    Returns unique citation numbers in ascending order.
    """
    citations = re.findall(r"\[(\d+)\]", answer)
    return sorted(set(citations), key=int)


# ──────────────────────────────── routes ────────────────────────────────── #

@app.route("/")
def home():
    """Simple homepage for the City Clerk RAG application."""
    # Build method options
    method_options = '<option value="unified">🧠 Unified Query Engine (Azure Search + Graph DB)</option>'
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>City Clerk RAG Assistant</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
        <style>
            .spinner-border-sm {{ width: 1rem; height: 1rem; }}
            .debug-panel {{ font-size: 0.8rem; }}
            .card-header {{ background-color: #f8f9fa; }}
        </style>
    </head>
    <body>
        <div class="container mt-5">
            <h1 class="text-center mb-4">📚 City Clerk RAG System</h1>
            
            <!-- Query Method Selector -->
            <div class="mb-3">
                <label class="form-label"><strong>Query Method:</strong></label>
                <div class="form-check">
                    <input class="form-check-input" type="radio" name="queryMethod" id="unifiedMethod" value="unified" checked>
                    <label class="form-check-label" for="unifiedMethod">
                        🧠 Unified Query Engine (Azure Search + Graph DB)
                    </label>
                </div>
                <div class="form-check">
                    <input class="form-check-input" type="radio" name="queryMethod" id="semanticMethod" value="semantic">
                    <label class="form-check-label" for="semanticMethod">
                        🔍 Original Semantic Search
                    </label>
                </div>
            </div>
            
            <!-- Search Form -->
            <form id="searchForm">
                <div class="input-group mb-3">
                    <input type="text" 
                           id="questionInput" 
                           class="form-control" 
                           placeholder="Enter your question about city documents..."
                           required>
                    <button type="submit" class="btn btn-primary" id="searchButton">
                        <span id="buttonText">Search</span>
                        <span id="loadingSpinner" class="spinner-border spinner-border-sm d-none" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </span>
                    </button>
                </div>
            </form>
            
            <!-- Debug Panel -->
            <div id="debugPanel" class="alert alert-info d-none">
                <h6>🔍 Debug Information</h6>
                <pre id="debugInfo"></pre>
            </div>
            
            <!-- Results Section -->
            <div id="results" class="d-none">
                <!-- Answer Card -->
                <div class="card mb-4">
                    <div class="card-header bg-success text-white">
                        <h5 class="mb-0">Answer</h5>
                    </div>
                    <div class="card-body">
                        <div id="answer"></div>
                        <div id="confidence" class="mt-2 text-muted"></div>
                    </div>
                </div>
                
                <!-- Source Documents -->
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">Source Documents</h5>
                    </div>
                    <div class="card-body">
                        <div id="chunks"></div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Enhanced JavaScript -->
        <script>
        document.getElementById('searchForm').addEventListener('submit', async (e) => {{
            e.preventDefault();
            
            const questionInput = document.getElementById('questionInput');
            const question = questionInput.value.trim();
            
            if (!question) {{
                alert('Please enter a question');
                return;
            }}
            
            // Get selected query method
            const queryMethod = document.querySelector('input[name="queryMethod"]:checked').value;
            
            // Show debug info
            const debugPanel = document.getElementById('debugPanel');
            const debugInfo = document.getElementById('debugInfo');
            debugPanel.classList.remove('d-none');
            debugInfo.textContent = `Sending request:\\nQuestion: "${{question}}"\\nMethod: ${{queryMethod}}\\nTimestamp: ${{new Date().toISOString()}}`;
            
            // UI feedback
            const searchButton = document.getElementById('searchButton');
            const buttonText = document.getElementById('buttonText');
            const loadingSpinner = document.getElementById('loadingSpinner');
            const results = document.getElementById('results');
            
            // Show loading state
            searchButton.disabled = true;
            buttonText.textContent = 'Searching...';
            loadingSpinner.classList.remove('d-none');
            results.classList.add('d-none');
            
            try {{
                console.log('Sending search request:', {{ question, query_method: queryMethod }});
                
                const response = await fetch('/search', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json',
                    }},
                    body: JSON.stringify({{
                        question: question,
                        query_method: queryMethod
                    }})
                }});
                
                const data = await response.json();
                console.log('Response received:', data);
                
                // Update debug info
                debugInfo.textContent += `\\n\\nResponse received:\\nStatus: ${{response.status}}\\nMethod used: ${{data.query_method || 'unknown'}}\\nConfidence: ${{(data.confidence * 100).toFixed(1)}}%`;
                
                if (!response.ok) {{
                    throw new Error(data.error || 'Search failed');
                }}
                
                // Display results
                displayResults(data);
                
            }} catch (error) {{
                console.error('Search error:', error);
                alert(`Error: ${{error.message}}`);
                
                // Update debug info with error
                debugInfo.textContent += `\\n\\nError: ${{error.message}}`;
                
            }} finally {{
                // Reset button state
                searchButton.disabled = false;
                buttonText.textContent = 'Search';
                loadingSpinner.classList.add('d-none');
            }}
        }});

        function displayResults(data) {{
            const results = document.getElementById('results');
            const answerDiv = document.getElementById('answer');
            const chunksDiv = document.getElementById('chunks');
            const confidenceDiv = document.getElementById('confidence');
            
            // Display answer
            answerDiv.innerHTML = marked.parse(data.answer || 'No answer found');
            
            // Display confidence if available
            if (data.confidence !== undefined) {{
                confidenceDiv.innerHTML = `<small>Confidence: ${{(data.confidence * 100).toFixed(1)}}%</small>`;
            }}
            
            // Display chunks
            chunksDiv.innerHTML = '';
            if (data.chunks && data.chunks.length > 0) {{
                data.chunks.forEach((chunk, index) => {{
                    const chunkCard = document.createElement('div');
                    chunkCard.className = 'card mb-2';
                    
                    const docTitle = chunk.doc ? chunk.doc.title : 'Unknown Document';
                    const docSource = chunk.doc ? chunk.doc.source : 'Unknown Source';
                    const similarity = chunk.similarity ? `${{(chunk.similarity * 100).toFixed(1)}}%` : 'N/A';
                    
                    chunkCard.innerHTML = `
                        <div class="card-header">
                            <small class="text-muted">
                                Document ${{index + 1}}: ${{docTitle}} | Source: ${{docSource}} | Relevance: ${{similarity}}
                            </small>
                        </div>
                        <div class="card-body">
                            <small>${{chunk.text}}</small>
                        </div>
                    `;
                    chunksDiv.appendChild(chunkCard);
                }});
            }} else {{
                chunksDiv.innerHTML = '<p class="text-muted">No source documents available</p>';
            }}
            
            // Show results
            results.classList.remove('d-none');
        }}
        </script>
    </body>
    </html>
    """
    return html

@app.route('/search', methods=['POST'])
def search():
    """Enhanced search endpoint with comprehensive debugging."""
    app.logger.info("="*80)
    app.logger.info("🔍 SEARCH REQUEST RECEIVED")
    app.logger.info("="*80)
    
    # Debug: Log raw request data
    app.logger.info(f"Request method: {request.method}")
    app.logger.info(f"Request headers: {dict(request.headers)}")
    app.logger.info(f"Request content type: {request.content_type}")
    
    # Try multiple ways to get the question
    question = None
    
    # Method 1: JSON body
    if request.is_json:
        data = request.get_json()
        app.logger.info(f"JSON data received: {data}")
        question = data.get('question', '') if data else ''
    
    # Method 2: Form data
    if not question and request.form:
        app.logger.info(f"Form data received: {dict(request.form)}")
        question = request.form.get('question', '')
    
    # Method 3: Query parameters
    if not question and request.args:
        app.logger.info(f"Query params received: {dict(request.args)}")
        question = request.args.get('question', '')
    
    app.logger.info(f"📝 Extracted question: '{question}'")
    app.logger.info("-"*80)
    
    if not question:
        app.logger.warning("❌ No question provided in request")
        return jsonify({'error': 'No question provided'}), 400
    
    try:
        app.logger.info(f"🎯 Processing question: {question}")
        
        # Check which query method is being used
        query_method = request.json.get('query_method', 'unified') if request.is_json else 'unified'
        app.logger.info(f"📊 Query method: {query_method}")
        
        if query_method == 'unified':
            # Use the new unified query engine
            app.logger.info("Using Unified Query Engine...")
            
            from scripts.graph_rag_stages.phase3_querying.debug_query_engine import DebugQueryEngine
            import asyncio
            
            # Initialize or get existing engine
            if not hasattr(app, 'query_engine'):
                app.logger.info("Initializing Debug Query Engine...")
                app.query_engine = DebugQueryEngine(
                    graph_dir=Path("simple_ner_graph"),
                    enable_debug=True
                )
                app.logger.info("✅ Query Engine initialized")
            
            # Execute query asynchronously
            result = asyncio.run(app.query_engine.query(question))
            
            # Ensure chunks have 'doc' key for compatibility
            chunks = result.get('chunks', [])
            if not chunks and result.get('answer'):
                # Create synthetic chunks if none exist
                chunks = [{
                    'text': result['answer'][:500],
                    'similarity': result.get('confidence', 0.5),
                    'doc': {
                        'title': 'Knowledge Graph Result',
                        'source': result.get('retrieval_method', 'GraphRAG')
                    }
                }]
            else:
                # Ensure all chunks have 'doc' key
                for chunk in chunks:
                    if 'doc' not in chunk:
                        chunk['doc'] = {
                            'title': 'Search Result',
                            'source': 'Knowledge Graph'
                        }
            
            app.logger.info(f"✅ Query completed. Answer length: {len(result.get('answer', ''))} chars")
            app.logger.info(f"📚 Chunks returned: {len(chunks)}")
            
            # Build prompt
            prompt = build_prompt(question, chunks)
            
            return jsonify({
                'answer': result.get('answer', 'No answer found'),
                'chunks': chunks,
                'prompt': prompt,
                'metadata': result.get('metadata', {}),
                'confidence': result.get('confidence', 0.0),
                'query_method': query_method
            })
            
        else:
            # Fallback to unified engine anyway since semantic search functions don't exist
            app.logger.info("Fallback: Using Unified Query Engine...")
            
            # Initialize or get existing engine
            if not hasattr(app, 'query_engine'):
                app.logger.info("Initializing Debug Query Engine...")
                app.query_engine = DebugQueryEngine(
                    graph_dir=Path("simple_ner_graph"),
                    enable_debug=True
                )
                app.logger.info("✅ Query Engine initialized")
            
            # Execute query asynchronously
            result = asyncio.run(app.query_engine.query(question))
            
            # Ensure chunks have 'doc' key for compatibility
            chunks = result.get('chunks', [])
            if not chunks and result.get('answer'):
                chunks = [{
                    'text': result['answer'][:500],
                    'similarity': result.get('confidence', 0.5),
                    'doc': {
                        'title': 'Knowledge Graph Result',
                        'source': result.get('retrieval_method', 'GraphRAG')
                    }
                }]
            else:
                for chunk in chunks:
                    if 'doc' not in chunk:
                        chunk['doc'] = {
                            'title': 'Search Result',
                            'source': 'Knowledge Graph'
                        }
            
            prompt = build_prompt(question, chunks)
            
            return jsonify({
                'answer': result.get('answer', 'No answer found'),
                'chunks': chunks,
                'prompt': prompt,
                'query_method': 'semantic'
            })
            
    except Exception as e:
        app.logger.error(f"❌ Search failed: {str(e)}")
        import traceback
        app.logger.error("Traceback:")
        app.logger.error(traceback.format_exc())
        app.logger.error("="*80)
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/test', methods=['GET', 'POST'])
def test_endpoint():
    """Test endpoint to verify system functionality."""
    
    test_question = "What are all the agenda documents?"
    
    app.logger.info("="*80)
    app.logger.info("🧪 TEST ENDPOINT CALLED")
    app.logger.info("="*80)
    
    try:
        from scripts.graph_rag_stages.phase3_querying.debug_query_engine import DebugQueryEngine
        import asyncio
        
        # Initialize engine
        engine = DebugQueryEngine(
            graph_dir=Path("simple_ner_graph"),
            enable_debug=True
        )
        
        # Get system stats
        stats = engine.get_system_stats()
        
        # Run test query
        result = asyncio.run(engine.query(test_question))
        
        return jsonify({
            'status': 'success',
            'test_question': test_question,
            'system_stats': stats,
            'test_result': {
                'answer': result.get('answer', '')[:200] + '...',
                'method': result.get('retrieval_method'),
                'confidence': result.get('confidence', 0)
            }
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'status': 'error',
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.get("/stats")
def stats():
    """Tiny ops endpoint—count total chunks."""
    resp = sb.table("documents_chunks").select("id", count="exact").execute()
    return jsonify({"total_chunks": resp.count})


@app.route('/debug/last-query', methods=['GET'])
def debug_last_query():
    """Debug the last query results."""
    
    from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
    import asyncio
    
    client = CosmosGraphClient()
    
    # Run the exact query that was generated
    query = "g.V().hasLabel('meeting').order().by('date', decr).limit(1).out('HAS_AGENDA').out('HAS_SECTION').out('HAS_AGENDA_ITEM').valueMap(true)"
    
    async def run_query():
        async with client:
            results = await client._execute_query(query)
            return results
    
    results = asyncio.run(run_query())
    
    return jsonify({
        'query': query,
        'result_count': len(results),
        'sample_results': results[:3] if results else [],
        'first_result_structure': list(results[0].keys()) if results and isinstance(results[0], dict) else [],
        'first_result_type': type(results[0]).__name__ if results else 'None'
    })


# ──────────────────────────────── main ─────────────────────────────────── #

if __name__ == "__main__":
    log.info("Starting Flask on 0.0.0.0:%s …", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=True)