"""
Document Linker
Links ordinance documents to their corresponding agenda items.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import json
from datetime import datetime
import PyPDF2
from openai import OpenAI
import os
from dotenv import load_dotenv
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

load_dotenv()

log = logging.getLogger('document_linker')


class DocumentLinker:
    """Links ordinance and resolution documents to agenda items."""
    
    def __init__(self,
                 openai_api_key: Optional[str] = None,
                 model: str = "gpt-4.1-mini-2025-04-14",
                 agenda_extraction_max_tokens: int = 32768):
        """Initialize the document linker."""
        self.api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not found in environment")
        
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
        self.agenda_extraction_max_tokens = agenda_extraction_max_tokens
    
    async def link_documents_for_meeting(self, 
                                       meeting_date: str,
                                       documents_dir: Path) -> Dict[str, List[Dict]]:
        """Find and link all documents for a specific meeting date."""
        log.info(f"🔗 Linking documents for meeting date: {meeting_date}")
        log.info(f"📁 Looking for ordinances in: {documents_dir}")
        
        # Create debug directory
        debug_dir = Path("city_clerk_documents/graph_json/debug")
        debug_dir.mkdir(exist_ok=True)
        
        # Convert date format: "01.09.2024" -> "01_09_2024"
        date_underscore = meeting_date.replace(".", "_")
        
        # Log what we're searching for
        with open(debug_dir / "document_search_info.txt", 'w') as f:
            f.write(f"Meeting Date: {meeting_date}\n")
            f.write(f"Date with underscores: {date_underscore}\n")
            f.write(f"Search directory: {documents_dir}\n")
            f.write(f"Search pattern: *{date_underscore}.pdf\n")
        
        # Find all matching ordinance/resolution files
        # Pattern: YYYY-## - MM_DD_YYYY.pdf
        pattern = f"*{date_underscore}.pdf"
        matching_files = list(documents_dir.glob(pattern))
        
        # Also try without spaces in case filenames vary
        pattern2 = f"*{date_underscore}*.pdf"
        additional_files = [f for f in documents_dir.glob(pattern2) if f not in matching_files]
        matching_files.extend(additional_files)
        
        # List all files in directory for debugging
        all_files = list(documents_dir.glob("*.pdf"))
        with open(debug_dir / "all_ordinance_files.txt", 'w') as f:
            f.write(f"Total files in {documents_dir}: {len(all_files)}\n")
            for file in sorted(all_files):
                f.write(f"  - {file.name}\n")
        
        log.info(f"📄 Found {len(matching_files)} documents for date {meeting_date}")
        
        if matching_files:
            log.info("📄 Documents found:")
            for f in matching_files[:5]:
                log.info(f"   - {f.name}")
            if len(matching_files) > 5:
                log.info(f"   ... and {len(matching_files) - 5} more")
        
        # Save matched files list
        with open(debug_dir / "matched_documents.txt", 'w') as f:
            f.write(f"Documents matching date {meeting_date}:\n")
            for file in matching_files:
                f.write(f"  - {file.name}\n")
        
        # Process documents in parallel
        linked_documents = {
            "ordinances": [],
            "resolutions": []
        }
        
        if matching_files:
            # Use asyncio.gather for parallel processing
            max_concurrent = min(multiprocessing.cpu_count() * 2, 10)
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_with_semaphore(doc_path):
                async with semaphore:
                    return await self._process_document(doc_path, meeting_date)
            
            # Process all documents concurrently
            results = await asyncio.gather(
                *[process_with_semaphore(doc_path) for doc_path in matching_files],
                return_exceptions=True
            )
            
            # Categorize results
            for doc_info, doc_path in zip(results, matching_files):
                if isinstance(doc_info, Exception):
                    log.error(f"Error processing {doc_path.name}: {doc_info}")
                    continue
                if doc_info:
                    if "ordinance" in doc_info.get("title", "").lower():
                        linked_documents["ordinances"].append(doc_info)
                    else:
                        linked_documents["resolutions"].append(doc_info)
        
        # Save linked documents info
        with open(debug_dir / "linked_documents.json", 'w') as f:
            json.dump(linked_documents, f, indent=2)
        
        log.info(f"✅ Linked {len(linked_documents['ordinances'])} ordinances, {len(linked_documents['resolutions'])} resolutions")
        return linked_documents
    
    async def _process_document(self, doc_path: Path, meeting_date: str) -> Optional[Dict[str, Any]]:
        """Process a single document to extract agenda item reference."""
        try:
            # Extract document number from filename (e.g., "2024-04" from "2024-04 - 01_23_2024.pdf")
            doc_match = re.match(r'^(\d{4}-\d{2})', doc_path.name)
            if not doc_match:
                log.warning(f"Could not parse document number from {doc_path.name}")
                return None
            
            document_number = doc_match.group(1)
            
            # Extract text from PDF
            text = self._extract_pdf_text(doc_path)
            if not text:
                log.warning(f"No text extracted from {doc_path.name}")
                return None
            
            # Extract agenda item code using LLM
            item_code = await self._extract_agenda_item_code(text, document_number)
            
            # Extract additional metadata
            parsed_data = self._parse_document_metadata(text)
            
            doc_info = {
                "path": str(doc_path),
                "filename": doc_path.name,
                "document_number": document_number,
                "item_code": item_code,
                "document_type": "Ordinance" if "ordinance" in text[:500].lower() else "Resolution",
                "title": self._extract_title(text),
                "parsed_data": parsed_data
            }
            
            log.info(f"📄 Processed {doc_path.name}: Item {item_code or 'NOT_FOUND'}")
            return doc_info
            
        except Exception as e:
            log.error(f"Error processing {doc_path.name}: {e}")
            return None
    
    def _extract_pdf_text(self, pdf_path: Path) -> str:
        """Extract text from PDF file."""
        try:
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                text_parts = []
                
                # Extract text from all pages
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
                
                return "\n".join(text_parts)
        except Exception as e:
            log.error(f"Failed to extract text from {pdf_path.name}: {e}")
            return ""
    
    async def _extract_agenda_item_code(self, text: str, document_number: str) -> Optional[str]:
        """Extract agenda item code from document text using LLM."""
        # Create debug directory if it doesn't exist
        debug_dir = Path("city_clerk_documents/graph_json/debug")
        debug_dir.mkdir(exist_ok=True)
        
        # Send the entire document to qwen-32b
        text_excerpt = text
        
        # Save the full text being sent to LLM for debugging
        with open(debug_dir / f"llm_input_{document_number}.txt", 'w', encoding='utf-8') as f:
            f.write(f"Document: {document_number}\n")
            f.write(f"Text length: {len(text)} characters\n")
            f.write(f"Using model: {self.model}\n")
            f.write(f"Max tokens: {self.agenda_extraction_max_tokens}\n")
            f.write("\n--- FULL DOCUMENT SENT TO LLM ---\n")
            f.write(text_excerpt)
        
        log.info(f"📄 Sending full document to LLM for {document_number}: {len(text)} characters")
        
        prompt = f"""You are analyzing a City of Coral Gables ordinance document (Document #{document_number}).

Your task is to find the AGENDA ITEM CODE referenced in this document.

IMPORTANT: The agenda item can appear ANYWHERE in the document - on page 3, at the end, or anywhere else. Search the ENTIRE document carefully.

The agenda item typically appears in formats like:
- (Agenda Item: E-1)
- Agenda Item: E-3)
- (Agenda Item E-1)
- Item H-3
- H.-3. (with periods and dots)
- E.-2. (with dots)
- E-2 (without dots)
- Item E-2

Full document text:
{text_excerpt}

Respond in this EXACT format:
AGENDA_ITEM: [code] or AGENDA_ITEM: NOT_FOUND

Important: Return the code as it appears (e.g., E-2, not E.-2.)

Examples:
- If you find "(Agenda Item: E-2)" → respond: AGENDA_ITEM: E-2
- If you find "Item E-2" → respond: AGENDA_ITEM: E-2
- If you find "H.-3." → respond: AGENDA_ITEM: H-3
- If no agenda item found → respond: AGENDA_ITEM: NOT_FOUND"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a precise data extractor. Find and extract only the agenda item code. Search the ENTIRE document thoroughly."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=self.agenda_extraction_max_tokens  # Use 100,000 for qwen
            )
            
            raw_response = response.choices[0].message.content.strip()
            
            # Save raw LLM response for debugging
            with open(debug_dir / f"llm_response_{document_number}_raw.txt", 'w', encoding='utf-8') as f:
                f.write(raw_response)
            
            # Parse response directly (no qwen parsing needed)
            result = raw_response
            
            # Log the response for debugging
            log.info(f"LLM response for {document_number} (first 200 chars): {result[:200]}")
            
            # Parse the response
            if "AGENDA_ITEM:" in result:
                code = result.split("AGENDA_ITEM:")[1].strip()
                if code != "NOT_FOUND":
                    # Normalize the code (remove dots, ensure format)
                    code = self._normalize_item_code(code)
                    log.info(f"✅ Found agenda item code for {document_number}: {code}")
                    return code
                else:
                    log.warning(f"❌ LLM could not find agenda item in {document_number}")
            else:
                log.error(f"❌ Invalid LLM response format for {document_number}: {result[:100]}")
            
            return None
            
        except Exception as e:
            log.error(f"Failed to extract agenda item for {document_number}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _normalize_item_code(self, code: str) -> str:
        """Normalize item code to consistent format."""
        if not code:
            return code
        
        # Remove trailing dots and spaces
        code = code.rstrip('. ')
        
        # Remove dots between letter and dash: "E.-1" -> "E-1"
        code = re.sub(r'([A-Z])\.(-)', r'\1\2', code)
        
        # Handle cases without dash: "E.1" -> "E-1"
        code = re.sub(r'([A-Z])\.(\d)', r'\1-\2', code)
        
        # Remove any remaining dots
        code = code.replace('.', '')
        
        return code
    
    def _extract_title(self, text: str) -> str:
        """Extract document title from text."""
        # Look for "AN ORDINANCE" or "A RESOLUTION" pattern
        title_match = re.search(r'(AN?\s+(ORDINANCE|RESOLUTION)[^.]+\.)', text[:2000], re.IGNORECASE)
        if title_match:
            return title_match.group(1).strip()
        
        # Fallback to first substantive line
        lines = text.split('\n')
        for line in lines[:20]:
            if len(line) > 20 and not line.isdigit():
                return line.strip()[:200]
        
        return "Untitled Document"
    
    def _parse_document_metadata(self, text: str) -> Dict[str, Any]:
        """Parse additional metadata from document."""
        metadata = {}
        
        # Extract date passed
        date_match = re.search(r'day\s+of\s+(\w+),?\s+(\d{4})', text)
        if date_match:
            metadata["date_passed"] = date_match.group(0)
        
        # Extract vote information
        vote_match = re.search(r'PASSED\s+AND\s+ADOPTED.*?(\d+).*?(\d+)', text, re.IGNORECASE | re.DOTALL)
        if vote_match:
            metadata["vote_details"] = {
                "ayes": vote_match.group(1),
                "nays": vote_match.group(2) if len(vote_match.groups()) > 1 else "0"
            }
        
        # Extract motion information
        motion_match = re.search(r'motion\s+(?:was\s+)?made\s+by\s+([^,]+)', text, re.IGNORECASE)
        if motion_match:
            metadata["motion"] = {"moved_by": motion_match.group(1).strip()}
        
        # Extract mayor signature
        mayor_match = re.search(r'Mayor[:\s]+([^\n]+)', text[-1000:])
        if mayor_match:
            metadata["signatories"] = {"mayor": mayor_match.group(1).strip()}
        
        return metadata 