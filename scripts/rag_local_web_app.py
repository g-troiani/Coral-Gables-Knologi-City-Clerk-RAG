#!/usr/bin/env python3
################################################################################
# File: rag_web_interface.py          (Supabase / pgvector demo – v2025‑05‑06)
################################################################################
"""
Mini Flask app that answers misophonia questions with Retrieval‑Augmented
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
from pathlib import Path        # (unused but left in to mirror original)
from typing import Dict, List
import json

from dotenv import load_dotenv
from flask import Flask, jsonify, request, make_response
from openai import OpenAI
from groq import Groq
from supabase import create_client
from flask_compress import Compress
from flask_cors import CORS

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
groq_client = Groq()  # For chat completions

app = Flask(__name__)
CORS(app)
app.config['COMPRESS_ALGORITHM'] = 'gzip'
Compress(app)

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


def semantic_search(
    query: str,
    *,
    limit: int = 8,
    threshold: float = 0.0,
) -> List[Dict]:
    """
    Retrieve candidate chunks via the pgvector RPC, then re-rank with an
    **explicit cosine similarity** so the final score is always in
    **[-100 … +100] percent**.

    Why the extra work?
    -------------------
    •  The SQL function returns a raw inner-product that can be > 1.  
       (embeddings are *not* unit-length.)  
    •  By pulling the real 1 536-D vectors and re-computing a cosine we get a
       true, bounded similarity that front-end code can safely show.

    The -100 … +100 range is produced by:  
        pct = clamp(cosine × 100, -100, 100)
    """
    # 1. Embed the query once and keep it cached
    q_vec = embed_cached(query)

    # 2. Fast ANN search in Postgres (over-fetch 4× so we can re-rank)
    rows = (
        sb.rpc(
            "match_documents_chunks",
            {
                "query_embedding": q_vec,
                "match_threshold": threshold,
                "match_count": limit * 4,
            },
        )
        .execute()
        .data
    ) or []

    # 3. Filter out bibliography-only chunks
    rows = [r for r in rows if not looks_like_refs(r["text"])]

    if not rows:
        return []

    # 4. Fetch document metadata (title, authors …) in one round-trip
    doc_ids = {r["document_id"] for r in rows}
    meta = {
        d["id"]: d
        for d in (
            sb.table("city_clerk_documents")
              .select("id,document_type,title,date,year,month,day,mayor,vice_mayor,commissioners,city_attorney,city_manager,city_clerk,public_works_director,agenda,keywords,source_pdf")
              .in_("id", list(doc_ids))
              .execute()
              .data
            or []
        )
    }

    # 5. Pull embeddings and page info once and compute **plain cosine** (no scaling)
    chunk_ids = [r["id"] for r in rows]

    emb_rows = (
        sb.table("documents_chunks")
          .select("id, embedding, page_start, page_end")
          .in_("id", chunk_ids)
          .execute()
          .data
    ) or []

    emb_map: Dict[str, List[float]] = {}
    page_map: Dict[str, Dict] = {}
    for e in emb_rows:
        raw = e["embedding"]
        if isinstance(raw, list):                    # list[Decimal]
            emb_map[e["id"]] = [float(x) for x in raw]
        elif isinstance(raw, str) and raw.startswith('['):   # TEXT  "[…]"
            emb_map[e["id"]] = [float(x) for x in raw.strip('[]').split(',')]
        
        # Store page info
        page_map[e["id"]] = {
            "page_start": e.get("page_start", 1),
            "page_end": e.get("page_end", 1)
        }

    for r in rows:
        vec = emb_map.get(r["id"])
        if vec:                                     # we now have the real vector
            cos = cosine_similarity(q_vec, vec)
            r["similarity"] = round(cos * 100, 1)   # –100…+100 % (or 0…100 %)
        else:                                       # fallback if something failed
            dist = float(r.get("similarity", 1.0))  # 0…2 cosine-distance
            r["similarity"] = round((1.0 - dist) * 100, 1)

        r["doc"] = meta.get(r["document_id"], {})
        
        # Add page info to the row
        page_info = page_map.get(r["id"], {"page_start": 1, "page_end": 1})
        r["page_start"] = page_info["page_start"]
        r["page_end"] = page_info["page_end"]

    # 6. Keep the top *limit* rows after proper re-ranking
    ranked = sorted(rows, key=lambda x: x["similarity"], reverse=True)[:limit]
    return ranked


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
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>City Clerk RAG Assistant</title>
        <style>
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                max-width: 800px; 
                margin: 0 auto; 
                padding: 2rem;
                line-height: 1.6;
                color: #333;
            }
            .header { 
                text-align: center; 
                margin-bottom: 2rem;
                padding-bottom: 1rem;
                border-bottom: 2px solid #e0e0e0;
            }
            .search-container {
                background: #f8f9fa;
                padding: 2rem;
                border-radius: 8px;
                margin: 2rem 0;
            }
            .search-box {
                width: 100%;
                padding: 1rem;
                border: 2px solid #ddd;
                border-radius: 4px;
                font-size: 16px;
                margin-bottom: 1rem;
            }
            .search-btn {
                background: #007bff;
                color: white;
                padding: 1rem 2rem;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 16px;
            }
            .search-btn:hover { background: #0056b3; }
            .results { margin-top: 2rem; }
            .answer { 
                background: white; 
                padding: 1.5rem; 
                border-radius: 8px; 
                border-left: 4px solid #007bff;
                margin: 1rem 0;
            }
            .sources { 
                background: #f8f9fa; 
                padding: 1rem; 
                border-radius: 4px; 
                margin-top: 1rem;
                font-size: 0.9em;
            }
            .loading { color: #666; font-style: italic; }
            .error { color: #dc3545; background: #f8d7da; padding: 1rem; border-radius: 4px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏛️ City Clerk RAG Assistant</h1>
            <p>Ask questions about city government documents, resolutions, ordinances, and meeting minutes</p>
        </div>
        
        <div class="search-container">
            <input type="text" id="queryInput" class="search-box" 
                   placeholder="Ask a question about city documents..." 
                   onkeypress="if(event.key==='Enter') search()">
            <button onclick="search()" class="search-btn">Search</button>
        </div>
        
        <div id="results" class="results"></div>
        
        <script>
            async function search() {
                const query = document.getElementById('queryInput').value.trim();
                if (!query) return;
                
                const resultsDiv = document.getElementById('results');
                resultsDiv.innerHTML = '<div class="loading">Searching...</div>';
                
                try {
                    const response = await fetch('/search', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ query: query })
                    });
                    
                    const data = await response.json();
                    
                    if (data.error) {
                        resultsDiv.innerHTML = `<div class="error">Error: ${data.error}</div>`;
                        return;
                    }
                    
                    let html = `<div class="answer">${data.answer.replace(/\\n/g, '<br>')}</div>`;
                    
                    if (data.results && data.results.length > 0) {
                        html += '<div class="sources"><strong>Sources:</strong><ul>';
                        data.results.forEach((result, i) => {
                            const doc = result.doc || {};
                            const title = doc.title || 'Untitled Document';
                            const similarity = Math.round(result.similarity || 0);
                            html += `<li>${title} (${similarity}% match)</li>`;
                        });
                        html += '</ul></div>';
                    }
                    
                    resultsDiv.innerHTML = html;
                } catch (error) {
                    resultsDiv.innerHTML = `<div class="error">Error: ${error.message}</div>`;
                }
            }
        </script>
    </body>
    </html>
    """
    return html

@app.post("/search")
def search():
    payload = request.get_json(force=True, silent=True) or {}
    question = (payload.get("query") or "").strip()
    if not question:
        return jsonify({"error": "Missing 'query'"}), 400

    try:
        # Retrieve semantic matches (client‑side cosine re‑ranked)
        raw_matches = semantic_search(question, limit=int(payload.get("limit", 8)))

        if not raw_matches:
            return jsonify(
                {
                    "answer": "I'm sorry, I don't have sufficient information to answer that.",
                    "citations": [],
                    "results": [],
                }
            )

        # ──────────────────── TRIM CHUNKS TO BUDGET ──────────────────── #
        chunks = trim_chunks(raw_matches)

        # ──────────────────── BUILD PROMPT & CALL LLM ─────────────────── #
        prompt = build_prompt(question, chunks)

        completion = groq_client.chat.completions.create(
            model="meta-llama/llama-4-maverick-17b-128e-instruct",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_completion_tokens=8192,
            top_p=1,
            stream=False,
            stop=None,
        )
        answer_text: str = completion.choices[0].message.content.strip()

        # ──────────────────── EXTRACT CITATIONS ──────────────────────── #
        citations = extract_citations(answer_text)

        # Remove embedding vectors before sending back to the browser
        for m in raw_matches:
            m.pop("embedding", None)

        # ──────────────────── RETURN JSON ─────────────────────────────── #
        response = jsonify(
            {
                "answer": answer_text,
                "citations": citations,
                "results": raw_matches,
            }
        )
        response.headers['Connection'] = 'keep-alive'
        return response
    except Exception as exc:  # noqa: BLE001
        log.exception("search failed")
        return jsonify({"error": str(exc)}), 500


@app.get("/stats")
def stats():
    """Tiny ops endpoint—count total chunks."""
    resp = sb.table("documents_chunks").select("id", count="exact").execute()
    return jsonify({"total_chunks": resp.count})


# ──────────────────────────────── main ─────────────────────────────────── #

if __name__ == "__main__":
    log.info("Starting Flask on 0.0.0.0:%s …", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=True)
