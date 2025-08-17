#!/usr/bin/env python3

import os
import json
from pathlib import Path
import sys
from dotenv import load_dotenv

# Ensure project root is on sys.path for package imports
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.graph_rag_stages.common.utils import get_llm_client

load_dotenv()

PROMPT_FILE = Path(__file__).parent / "old_ner_prompt.txt"
ONTOLOGY_FILE = Path(__file__).parent / "ontology_context.txt"


def load_prompts_from_file() -> tuple[str, str]:
    text = PROMPT_FILE.read_text(encoding='utf-8')
    parts = text.split("=== PROMPT 1 — ENTITIES ONLY ===")
    system_part = parts[0].replace("SYSTEM TEMPLATE", "").strip()
    # For our use, we only need Prompt 1 (entities)
    user_part = parts[1].split("=== PROMPT 2", 1)[0].strip()
    return system_part, user_part


def parse_chunk_file(chunk_file: str):
    text = Path(chunk_file).read_text(encoding='utf-8')
    meta = {}
    header_parts = text.split("---")
    header_text = "\n".join(header_parts[:2]) if len(header_parts) >= 2 else header_parts[0]
    for line in header_text.splitlines():
        if line.startswith('#') and ':' in line:
            key, val = line[1:].split(':', 1)
            meta[key.strip()] = val.strip()
    chunk_id = meta.get('Chunk', meta.get('Chunk ID', 'unknown'))
    document = meta.get('Document', meta.get('Source', 'unknown'))
    document_type = meta.get('Document_Type', meta.get('Document Type', 'unknown')).lower()
    meeting_date = meta.get('Meeting_Date', meta.get('Meeting Date', 'unknown'))
    source_file_name = meta.get('Source_File_Name', Path(chunk_file).name)
    body_text = header_parts[-1].strip() if header_parts else text
    return {
        'chunk_id': chunk_id or 'unknown',
        'document': document or 'unknown',
        'document_type': document_type or 'unknown',
        'meeting_date': meeting_date or 'unknown',
        'Source_File_Name': source_file_name,
        'chunk_file': Path(chunk_file).name,
    }, body_text


def extract_entities(chunk_text: str, document_type: str, meeting_date: str, source_file: str):
    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()
    if not model:
        raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")

    system_prompt, user_template = load_prompts_from_file()
    ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')

    # Fill Prompt 1 placeholders
    user_prompt = (user_template
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", str(chunk_text[:3000]))
    )
    # Minimal entity buckets placeholder if present
    if "{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}" in user_prompt:
        buckets = []
        # A minimal list to avoid very long prompts; can be expanded if needed
        buckets_types = [
            "Person","Organization","Document","AgendaDocument","Section","AgendaItem",
            "Policy","Contract","Technology","VoteOutcome","Event","Location","Asset","Project","Role","Topic","Action"
        ]
        for t in buckets_types:
            buckets.append(f'"{t}": []')
        user_prompt = user_prompt.replace("{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}", ", ".join(buckets))

    # Prepend ontology context to the user prompt
    user_prompt_full = f"{ontology_context}\n\n{user_prompt}"

    # Print prompts for inspection
    print("\n=== SYSTEM PROMPT ===\n" + system_prompt)
    print("\n=== USER PROMPT (with ontology) ===\n" + user_prompt_full)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt_full}
        ],
        temperature=0,
        max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
    )
    result_text = (response.choices[0].message.content or '').strip()
    try:
        parsed = json.loads(result_text)
    except json.JSONDecodeError:
        parsed = {"entities": {}, "relationships": []}
    return parsed, result_text


if __name__ == "__main__":
    chunk_path = "simple_ner_graph/document_chunks/461881bb58f6_agenda_01_09_2024.txt"
    meta, text = parse_chunk_file(chunk_path)
    result, raw_text = extract_entities(
        text,
        document_type=meta.get('document_type', 'unknown'),
        meeting_date=meta.get('meeting_date', 'unknown'),
        source_file=meta.get('Source_File_Name', 'unknown'),
    )
    # Save raw LLM response alongside script
    (Path(__file__).parent / "llm_entity_extraction_output.txt").write_text(raw_text, encoding='utf-8')
    print("\n=== LLM RAW RESULT ===\n" + json.dumps(result, indent=2, ensure_ascii=False))
