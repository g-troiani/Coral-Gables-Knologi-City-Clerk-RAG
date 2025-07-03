#!/usr/bin/env python3
"""
Enhanced Document Linker - Hierarchical Legal Document Processing

This module implements sophisticated ordinance and resolution extraction with:
- Hybrid regex + LLM agenda item code extraction
- Rich legal metadata parsing (votes, motions, signatures)  
- Explicit hierarchical relationship creation
- GraphRAG-optimized output with entity hints
- Parallel processing with rate limiting
"""

import re
import json
import logging
import asyncio
import multiprocessing
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import os
from openai import AzureOpenAI

from .stage1_pdf_ocr import PDFOCRExtractor
from scripts.graph_rag_stages.common.utils import get_llm_client, call_llm_with_retry

log = logging.getLogger(__name__)


class EnhancedDocumentLinker:
    """Enhanced processor for ordinances and resolutions with hierarchical linking."""
    
    def __init__(self, output_dir: Path = Path("city_clerk_documents/extracted_json")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        self.pdf_extractor = PDFOCRExtractor(output_dir)
        
        # Initialize LLM client for agenda item extraction
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o")
        
    async def process_legal_documents(self, base_dir: Path, meeting_date: str) -> Dict[str, Any]:
        """
        Process all ordinances and resolutions for a meeting with hierarchical linking.
        
        Args:
            base_dir: Base directory containing Ordinances/ and Resolutions/ subdirectories
            meeting_date: Meeting date in format "01.09.2024"
            
        Returns:
            Dictionary containing processed documents and hierarchical relationships
        """
        log.info(f"📜 Processing legal documents for meeting: {meeting_date}")
        
        # Discover ordinance and resolution files
        ordinance_files = self._discover_ordinances(base_dir, meeting_date)
        resolution_files = self._discover_resolutions(base_dir, meeting_date)
        
        total_files = len(ordinance_files) + len(resolution_files)
        if total_files == 0:
            log.warning(f"No legal documents found for {meeting_date}")
            return self._empty_result(meeting_date)
        
        log.info(f"📄 Found {len(ordinance_files)} ordinances and {len(resolution_files)} resolutions")
        
        # Process documents in parallel with rate limiting
        max_concurrent = min(multiprocessing.cpu_count(), 8)
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_semaphore(doc_path, doc_type):
            async with semaphore:
                return await self._process_document(doc_path, meeting_date, doc_type)
        
        # Create tasks for all documents
        tasks = []
        for doc_path in ordinance_files:
            tasks.append(process_with_semaphore(doc_path, "ordinance"))
        for doc_path in resolution_files:
            tasks.append(process_with_semaphore(doc_path, "resolution"))
        
        # Process all documents concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful results
        processed_documents = []
        hierarchical_relationships = []
        
        for result in results:
            if isinstance(result, Exception):
                log.error(f"❌ Document processing failed: {result}")
                continue
            if result:
                processed_documents.append(result['document_data'])
                hierarchical_relationships.extend(result['relationships'])
        
        # Build comprehensive result
        result = {
            "meeting_date": meeting_date,
            "document_type": "legal_document_collection",
            "documents": processed_documents,
            "hierarchical_relationships": hierarchical_relationships,
            "summary": self._build_summary(processed_documents),
            "metadata": {
                "extraction_method": "enhanced_hybrid_legal_extraction",
                "total_files_discovered": total_files,
                "successfully_processed": len(processed_documents),
                "processing_timestamp": datetime.now().isoformat()
            }
        }
        
        # Save comprehensive result
        self._save_legal_document_collection(result, meeting_date)
        
        log.info(f"✅ Processed {len(processed_documents)} legal documents with hierarchical linking")
        return result
    
    def _discover_ordinances(self, base_dir: Path, meeting_date: str) -> List[Path]:
        """Discover ordinance files using flexible patterns."""
        date_underscore = meeting_date.replace(".", "_")
        
        search_dirs = [
            base_dir / "Ordinances",
            base_dir / "Ordinances" / "2024",
            base_dir / "ordinances"  # lowercase variant
        ]
        
        ordinance_files = []
        for search_dir in search_dirs:
            if search_dir.exists():
                # Multiple pattern matching for robustness
                patterns = [
                    f"*{date_underscore}.pdf",
                    f"*{date_underscore}*.pdf",
                    f"*{meeting_date.replace('.', '-')}*.pdf"
                ]
                
                for pattern in patterns:
                    matches = list(search_dir.rglob(pattern))
                    ordinance_files.extend(matches)
        
        return sorted(list(set(ordinance_files)))
    
    def _discover_resolutions(self, base_dir: Path, meeting_date: str) -> List[Path]:
        """Discover resolution files using flexible patterns."""
        date_underscore = meeting_date.replace(".", "_")
        
        search_dirs = [
            base_dir / "Resolutions",
            base_dir / "Resolutions" / "2024",
            base_dir / "resolutions"  # lowercase variant
        ]
        
        resolution_files = []
        for search_dir in search_dirs:
            if search_dir.exists():
                # Multiple pattern matching for robustness
                patterns = [
                    f"*{date_underscore}.pdf",
                    f"*{date_underscore}*.pdf", 
                    f"*{meeting_date.replace('.', '-')}*.pdf"
                ]
                
                for pattern in patterns:
                    matches = list(search_dir.rglob(pattern))
                    resolution_files.extend(matches)
        
        return sorted(list(set(resolution_files)))
    
    async def _process_document(self, doc_path: Path, meeting_date: str, doc_type: str) -> Optional[Dict[str, Any]]:
        """Process a single legal document with hierarchical linking."""
        log.info(f"📝 Processing {doc_type}: {doc_path.name}")
        
        # Extract document number from filename
        doc_match = re.match(r'^(\d{4}-\d{2,3})', doc_path.name)
        if not doc_match:
            log.warning(f"Could not parse document number from {doc_path.name}")
            return None
        
        document_number = doc_match.group(1)
        
        try:
            # Stage 1: OCR extraction
            ocr_result = self.pdf_extractor.extract_pdf(doc_path)
            
            if not ocr_result.get('full_text'):
                log.warning(f"No text extracted from {doc_path.name}")
                return None
            
            # Stage 2: Extract agenda item code using hybrid approach
            agenda_item_code = await self._extract_agenda_item_code(
                ocr_result['full_text'], document_number, doc_type
            )
            
            # Stage 3: Extract document title
            title = self._extract_title(ocr_result['full_text'], doc_type)
            
            # Stage 4: Parse rich legal metadata
            legal_metadata = self._parse_legal_metadata(ocr_result['full_text'], doc_type)
            
            # Build document data structure
            document_data = {
                "id": f"{doc_type}-{document_number}",
                "document_type": doc_type,
                "document_number": document_number,
                "source_file": doc_path.name,
                "file_path": str(doc_path),
                "meeting_date": meeting_date,
                "agenda_item_code": agenda_item_code,
                "title": title,
                "full_text": ocr_result['full_text'],
                "pages": ocr_result['pages'],
                "legal_metadata": legal_metadata,
                "metadata": {
                    **ocr_result['metadata'],
                    "extraction_method": "enhanced_hybrid_legal_extraction",
                    "hierarchical_structure": True,
                    "agenda_item_extraction": "regex_llm_hybrid"
                }
            }
            
            # Generate hierarchical relationships
            relationships = self._create_hierarchical_relationships(document_data, meeting_date)
            
            # Save individual document with enhanced format
            self._save_individual_document(document_data, doc_type)
            
            # Generate enhanced markdown
            self._generate_enhanced_markdown(document_data, doc_path, doc_type)
            
            return {
                "document_data": document_data,
                "relationships": relationships
            }
            
        except Exception as e:
            log.error(f"❌ Failed to process {doc_path.name}: {e}")
            return None
    
    async def _extract_agenda_item_code(self, text: str, document_number: str, doc_type: str) -> Optional[str]:
        """Extract agenda item code using hybrid regex + LLM approach."""
        
        # Stage 1: Try regex extraction (fast path)
        regex_result = self._extract_agenda_code_regex(text)
        if regex_result:
            log.info(f"✅ Found agenda item code via regex for {document_number}: {regex_result}")
            return regex_result
        
        # Stage 2: LLM extraction (fallback path)
        log.info(f"🧠 Using LLM fallback for agenda item extraction: {document_number}")
        llm_result = await self._extract_agenda_code_llm(text, document_number, doc_type)
        if llm_result:
            log.info(f"✅ Found agenda item code via LLM for {document_number}: {llm_result}")
            return llm_result
        
        log.warning(f"❌ Could not find agenda item code for {document_number}")
        return None
    
    def _extract_agenda_code_regex(self, text: str) -> Optional[str]:
        """Extract agenda item code using regex patterns."""
        patterns = [
            r'Agenda\s+Item[:\s]+([A-Z]-?\d+)(?:\)|\.)?',    # Agenda Item: E-1) or E-1.
            r'Item\s+([A-Z]\.-?\d+\.?)',                     # Item D.-1.
            r'Agenda\s+Item[:\s]+([A-Z]\.-?\d+\.?)',         # Agenda Item: D.-1.
            r'Section\s+([A-Z])[,\s]+Item\s+(\d+)',          # Section D, Item 1
            r'consent\s+agenda.*item\s+([A-Z]\.-?\d+\.?)',   # Consent Agenda ... Item D.-1.
            r'\b([A-Z]\.-?\d+\.?)\s+\d{2}-\d{4}',           # D.-1. 23-6830 pattern
            r'agenda\s+item[:\s]*([A-Z]-?\d+)(?:\)|\.)?',    # agenda item: E-1) or E-1.
            r'item\s+([A-Z]-\d+)',                           # item E-1
            r'relating\s+to\s+agenda\s+item\s+([A-Z]-\d+)',  # relating to agenda item E-1
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:  # Section X, Item Y format
                    code = f"{match.group(1)}-{match.group(2)}"
                else:
                    code = match.group(1)
                return self._normalize_item_code(code)
        
        return None
    
    async def _extract_agenda_code_llm(self, text: str, document_number: str, doc_type: str) -> Optional[str]:
        """Extract agenda item code using LLM analysis."""
        
        doc_type_text = doc_type.capitalize()
        
        prompt = f"""You are analyzing a City of Coral Gables {doc_type} document (Document #{document_number}).

Your task is to find the AGENDA ITEM CODE referenced in this document.

CRITICAL INSTRUCTIONS:
1. Search the ENTIRE document for agenda item references
2. Return ONLY the code in this format: AGENDA_ITEM: [code]
3. The code should be ONLY the letter and number (e.g., E-2, F-10, H-1)
4. Do NOT include any explanations, reasoning, or additional text
5. If no agenda item is found, return: AGENDA_ITEM: NOT_FOUND

Examples of valid responses:
- AGENDA_ITEM: E-2
- AGENDA_ITEM: F-10
- AGENDA_ITEM: H-1
- AGENDA_ITEM: NOT_FOUND

DO NOT RETURN ANYTHING ELSE. NO EXPLANATIONS.

Full document text:
{text}"""
        
        messages = [
            {
                "role": "system",
                "content": f"You are a precise data extractor for {doc_type_text} documents. Find and extract only the agenda item code. Search the ENTIRE document thoroughly."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        try:
            result = await call_llm_with_retry(
                self.client,
                messages,
                model=self.model,
                temperature=0,
                max_tokens=100
            )
            
            result = result.strip()
            log.debug(f"🤖 LLM response for {document_number}: {result}")
            
            # Parse the response
            if "AGENDA_ITEM:" in result:
                # Extract just the code part
                parts = result.split("AGENDA_ITEM:")[1].strip()
                
                # Extract just the code pattern (letter-number)
                code_match = re.match(r'^([A-Z]-?\d+)', parts)
                if code_match:
                    code = code_match.group(1)
                    if code != "NOT_FOUND":
                        return self._normalize_item_code(code)
                elif parts.startswith("NOT_FOUND"):
                    return None
                else:
                    # Try to extract code from a messy response
                    code_pattern = r'\b([A-Z]-?\d+)\b'
                    match = re.search(code_pattern, parts)
                    if match:
                        return self._normalize_item_code(match.group(1))
            
            return None
            
        except Exception as e:
            log.error(f"❌ LLM extraction failed for {document_number}: {e}")
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
        
        # Ensure we have a dash between letter and number
        code = re.sub(r'([A-Z])(\d)', r'\1-\2', code)
        
        return code.upper()
    
    def _extract_title(self, text: str, doc_type: str) -> str:
        """Extract document title from text."""
        if doc_type == "ordinance":
            pattern = r'(AN?\s+ORDINANCE[^.]+\.)'
        else:  # resolution
            pattern = r'(A\s+RESOLUTION[^.]+\.)'
            
        title_match = re.search(pattern, text[:2000], re.IGNORECASE)
        if title_match:
            return title_match.group(1).strip()
        
        # Fallback to first substantive line
        lines = text.split('\n')
        for line in lines[:20]:
            if len(line) > 20 and not line.isdigit():
                return line.strip()[:200]
        
        return f"Untitled {doc_type.capitalize()}"
    
    def _parse_legal_metadata(self, text: str, doc_type: str) -> Dict[str, Any]:
        """Parse additional legal metadata from document."""
        metadata = {
            "document_type": doc_type
        }
        
        # Extract date passed
        date_match = re.search(r'day\s+of\s+(\w+),?\s+(\d{4})', text)
        if date_match:
            metadata["date_passed"] = date_match.group(0)
        
        # PHASE 1: Extract reading status for ordinances and resolutions using regex patterns
        passed_first_reading = False
        passed_second_reading = False
        outcome_status = "Pending"
        
        # Check for first reading patterns
        first_reading_patterns = [
            r'Passed\s+on\s+First\s+Reading',
            r'PASSED\s+ON\s+FIRST\s+READING',
            r'first\s+reading.*passed',
            r'adopted\s+on\s+first\s+reading'
        ]
        
        for pattern in first_reading_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                passed_first_reading = True
                break
        
        # Check for final passage/second reading patterns
        final_passage_patterns = [
            r'PASSED\s+AND\s+ADOPTED',
            r'passed\s+and\s+adopted',
            r'ADOPTED\s+THIS.*DAY\s+OF',
            r'adopted\s+this.*day\s+of',
            r'Passed\s+on\s+Second\s+Reading',
            r'PASSED\s+ON\s+SECOND\s+READING',
            r'second\s+reading.*passed'
        ]
        
        for pattern in final_passage_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                passed_second_reading = True
                break
        
        # Extract vote information
        vote_match = re.search(r'(?:Yeas?|Ayes?):\s*([^)]+)\)', text, re.IGNORECASE)
        if vote_match:
            yeas = vote_match.group(1)
            metadata["vote_details"] = {"yeas": yeas}
            
            # Look for nays
            nays_match = re.search(r'(?:Nays?|Nos?):\s*([^)]+)\)', text, re.IGNORECASE)
            if nays_match:
                metadata["vote_details"]["nays"] = nays_match.group(1)
            
            # Look for unanimous
            if "unanimous" in text.lower():
                metadata["vote_details"]["unanimous"] = True
            
            # Determine outcome status based on vote information
            if passed_second_reading:
                outcome_status = "Passed"
            elif passed_first_reading:
                outcome_status = "First Reading Passed"
            elif vote_match:  # Has vote info but no clear passage indication
                # Check if there are nays or if vote failed
                nays_text = metadata["vote_details"].get("nays", "").strip().lower()
                if nays_text and nays_text not in ["none", "absent", ""]:
                    # Count votes to determine if passed
                    yeas_count = len([name.strip() for name in yeas.split(',') if name.strip()])
                    nays_count = len([name.strip() for name in nays_text.split(',') if name.strip() and name.strip() not in ["none", "absent"]])
                    if yeas_count > nays_count:
                        outcome_status = "Passed" if passed_second_reading else "First Reading Passed"
                    else:
                        outcome_status = "Failed"
                else:
                    # Unanimous or all ayes
                    outcome_status = "Passed" if passed_second_reading else "First Reading Passed"
        
        # PHASE 2: LLM Validation and Enhancement
        try:
            llm_metadata = self._validate_with_llm(text, doc_type, {
                "passed_first_reading": passed_first_reading,
                "passed_second_reading": passed_second_reading,
                "outcome_status": outcome_status,
                "vote_details": metadata.get("vote_details", {})
            })
            
            # Use LLM results if they provide additional confidence
            if llm_metadata:
                # LLM can override if it found something regex missed
                if llm_metadata.get("confidence_score", 0) > 0.8:
                    passed_first_reading = llm_metadata.get("passed_first_reading", passed_first_reading)
                    passed_second_reading = llm_metadata.get("passed_second_reading", passed_second_reading)
                    outcome_status = llm_metadata.get("outcome_status", outcome_status)
                    
                    # Merge additional vote details if found
                    if llm_metadata.get("vote_details"):
                        vote_details = metadata.get("vote_details", {})
                        vote_details.update(llm_metadata["vote_details"])
                        metadata["vote_details"] = vote_details
                
                # Always store LLM reasoning for debugging
                metadata["llm_analysis"] = {
                    "reasoning": llm_metadata.get("reasoning", ""),
                    "confidence": llm_metadata.get("confidence_score", 0),
                    "method": "llm_validation"
                }
        
        except Exception as e:
            log.warning(f"LLM validation failed for {doc_type}, using regex results: {e}")
            metadata["llm_analysis"] = {
                "reasoning": f"LLM validation failed: {e}",
                "confidence": 0,
                "method": "regex_only"
            }
        
        # Add reading status to metadata
        metadata["passed_first_reading"] = passed_first_reading
        metadata["passed_second_reading"] = passed_second_reading
        metadata["outcome_status"] = outcome_status
        
        # Extract motion information
        motion_match = re.search(r'Moved:\s*([^/]+)', text, re.IGNORECASE)
        if motion_match:
            moved_by = motion_match.group(1).strip()
            metadata["motion"] = {"moved_by": moved_by}
            
            # Look for seconded
            second_match = re.search(r'Seconded:\s*([^)]+)', text, re.IGNORECASE)
            if second_match:
                metadata["motion"]["seconded_by"] = second_match.group(1).strip()
        
        # Extract mayor signature
        mayor_match = re.search(r'(?:APPROVED:|MAYOR)\s*([A-Z\s]+(?:MAYOR)?)', text[-1000:])
        if mayor_match:
            metadata["signatories"] = {"mayor": mayor_match.group(1).strip()}
        
        # Resolution-specific metadata
        if doc_type == "resolution":
            whereas_matches = re.findall(r'WHEREAS,?\s+([^;]+)', text[:3000], re.IGNORECASE)
            if whereas_matches:
                metadata["whereas_clauses"] = whereas_matches[:3]  # First 3 clauses
        
        return metadata
    
    def _validate_with_llm(self, text: str, doc_type: str, regex_results: Dict) -> Optional[Dict]:
        """Use LLM to validate and enhance legal metadata extraction."""
        
        prompt = f"""You are analyzing a City of Coral Gables {doc_type} document to extract voting and passage information.

DOCUMENT TEXT (first 3000 characters):
{text[:3000]}

REGEX EXTRACTION RESULTS:
- Passed First Reading: {regex_results.get('passed_first_reading', False)}
- Passed Second Reading: {regex_results.get('passed_second_reading', False)}  
- Outcome Status: {regex_results.get('outcome_status', 'Pending')}
- Vote Details: {regex_results.get('vote_details', {})}

INSTRUCTIONS:
1. Analyze the document text to determine the ACTUAL status
2. Look for any voting information, reading status, or outcome indicators
3. Provide your analysis in this EXACT JSON format:

{{
    "passed_first_reading": true/false,
    "passed_second_reading": true/false,
    "outcome_status": "Passed|Failed|First Reading Passed|Second Reading Passed|Deferred|Tabled|Pending",
    "vote_details": {{
        "yeas": "comma-separated names or count",
        "nays": "comma-separated names or count", 
        "unanimous": true/false,
        "abstentions": "if any"
    }},
    "confidence_score": 0.0-1.0,
    "reasoning": "Brief explanation of your analysis and any corrections to regex results"
}}

CRITICAL RULES:
- Only return valid JSON
- For ordinances, distinguish between first reading and final adoption
- For resolutions, they typically pass in one reading unless stated otherwise
- Look for phrases like "PASSED AND ADOPTED", "unanimous", "first reading", etc.
- If vote details show names, count them for outcome determination
- Confidence score should be 0.9+ if you're very certain, 0.5-0.8 if partially certain, <0.5 if unclear"""

        try:
            messages = [
                {
                    "role": "system", 
                    "content": f"You are a legal document analyzer specializing in City of Coral Gables {doc_type} documents. Extract precise voting and passage information."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            # Use the existing client from the class  
            result = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0,
                max_tokens=800
            )
            
            # Parse JSON response
            import json
            if result and result.choices:
                response_text = result.choices[0].message.content.strip()
                
                # Clean the response
                if response_text.startswith('```json'):
                    response_text = response_text[7:]
                if response_text.endswith('```'):
                    response_text = response_text[:-3]
                response_text = response_text.strip()
                
                parsed = json.loads(response_text)
                
                # Validate the response structure
                required_fields = ["passed_first_reading", "passed_second_reading", "outcome_status", "confidence_score"]
                if all(field in parsed for field in required_fields):
                    log.info(f"LLM validation successful with confidence {parsed.get('confidence_score', 0)}")
                    return parsed
                else:
                    log.warning("LLM response missing required fields")
                    return None
            
        except Exception as e:
            log.error(f"LLM validation error: {e}")
            return None
    
    def _create_hierarchical_relationships(self, document_data: Dict[str, Any], meeting_date: str) -> List[Dict[str, Any]]:
        """Create hierarchical relationships for the legal document."""
        relationships = []
        
        agenda_item_code = document_data.get('agenda_item_code')
        if not agenda_item_code:
            return relationships
        
        # Create relationships
        meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
        agenda_item_id = f"item-{meeting_date.replace('.', '-')}-{agenda_item_code}"
        document_id = document_data['id']
        
        # Meeting → AgendaItem relationship (may already exist)
        relationships.append({
            "source": meeting_id,
            "target": agenda_item_id,
            "relationship": "HAS_AGENDA_ITEM",
            "properties": {
                "item_code": agenda_item_code,
                "meeting_date": meeting_date
            }
        })
        
        # AgendaItem → LegalDocument relationship
        relationships.append({
            "source": agenda_item_id,
            "target": document_id,
            "relationship": "IMPLEMENTS",
            "properties": {
                "document_type": document_data['document_type'],
                "document_number": document_data['document_number'],
                "implementation_type": "legal_document"
            }
        })
        
        return relationships
    
    def _save_individual_document(self, document_data: Dict[str, Any], doc_type: str) -> None:
        """Save individual legal document data."""
        filename = f"{document_data['source_file'].replace('.pdf', '')}_enhanced_{doc_type}.json"
        output_path = self.output_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(document_data, f, indent=2, ensure_ascii=False)
        
        log.debug(f"💾 Saved enhanced {doc_type}: {output_path}")
    
    def _generate_enhanced_markdown(self, document_data: Dict[str, Any], doc_path: Path, doc_type: str) -> None:
        """Generate enhanced markdown with GraphRAG optimization."""
        
        item_code = document_data.get('agenda_item_code', 'N/A')
        doc_number = document_data.get('document_number', 'N/A')
        
        # Build GraphRAG-optimized header with entity hints
        enhanced_header = f"""---
ENTITIES IN THIS DOCUMENT:
- AGENDA_ITEM: {item_code}
- {doc_type.upper()}: {doc_number}
- DOCUMENT_TYPE: {doc_type.upper()}

---

**THIS DOCUMENT CONTAINS:**
The following entities should be extracted:
- Agenda Item {item_code} (entity type: agenda_item)
- {doc_type.capitalize()} {doc_number} (entity type: {doc_type})
- Meeting Date: {document_data.get('meeting_date', 'N/A')} (entity type: meeting)

**EXAMPLE EXTRACTION:**
From the text "relating to agenda item {item_code}", extract:
- Entity: "{item_code}", Type: "agenda_item"

From the text "{doc_type} {doc_number}", extract:
- Entity: "{doc_number}", Type: "{doc_type}"

---

DOCUMENT METADATA AND CONTEXT
=============================

**DOCUMENT IDENTIFICATION:**
- Full Path: {doc_type.capitalize()}s/2024/{doc_path.name}
- Document Type: {doc_type.upper()}
- Filename: {doc_path.name}

**PARSED INFORMATION:**
- Document Number: {doc_number}
- Meeting Date: {document_data.get('meeting_date', 'N/A')}
- Related Agenda Item: {item_code}
- Title: {document_data.get('title', 'N/A')}

**SEARCHABLE IDENTIFIERS:**
- DOCUMENT_NUMBER: {doc_number}
- MEETING_DATE: {document_data.get('meeting_date', 'N/A')}
- AGENDA_ITEM: {item_code}
- DOCUMENT_TYPE: {doc_type.upper()}

**NATURAL LANGUAGE DESCRIPTION:**
This is {doc_type.capitalize()} {doc_number} from the {document_data.get('meeting_date', '')} City Commission meeting, relating to agenda item {item_code}.

**QUERY HELPERS:**
- To find information about {item_code}, search for 'Item {item_code}' or '{item_code}'
- To find this document, search for '{doc_number}'
- This {doc_type} {self._get_doc_type_description(doc_type)}

---

## What is Item {item_code}?
Item {item_code} is implemented by this {doc_type}.
{item_code} refers to {doc_type} {doc_number}.

**RELATIONSHIP**: {doc_number} implements agenda item {item_code}.

---

# ORIGINAL DOCUMENT CONTENT

{document_data.get('full_text', '')}
"""
        
        # Save enhanced markdown
        markdown_filename = f"{document_data['source_file'].replace('.pdf', '')}_enhanced_{doc_type}.md"
        markdown_path = self.output_dir / markdown_filename
        
        with open(markdown_path, 'w', encoding='utf-8') as f:
            f.write(enhanced_header)
        
        log.debug(f"📝 Generated enhanced markdown: {markdown_path}")
    
    def _get_doc_type_description(self, doc_type: str) -> str:
        """Get description for document type."""
        descriptions = {
            'ordinance': 'modifies city code and requires multiple readings',
            'resolution': 'expresses city policy or authorizes specific actions'
        }
        return descriptions.get(doc_type, 'is an official city document')
    
    def _build_summary(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build summary statistics for processed documents."""
        summary = {
            "total_documents": len(documents),
            "by_type": {},
            "linked_to_agenda": 0,
            "unlinked_documents": 0
        }
        
        for doc in documents:
            doc_type = doc['document_type']
            summary['by_type'][doc_type] = summary['by_type'].get(doc_type, 0) + 1
            
            if doc.get('agenda_item_code'):
                summary['linked_to_agenda'] += 1
            else:
                summary['unlinked_documents'] += 1
        
        return summary
    
    def _save_legal_document_collection(self, result: Dict[str, Any], meeting_date: str) -> None:
        """Save the complete legal document collection."""
        filename = f"{meeting_date.replace('.', '_')}_enhanced_legal_documents.json"
        output_path = self.output_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        log.info(f"💾 Saved enhanced legal document collection: {output_path}")
    
    def _empty_result(self, meeting_date: str) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "meeting_date": meeting_date,
            "document_type": "legal_document_collection",
            "documents": [],
            "hierarchical_relationships": [],
            "summary": {"total_documents": 0},
            "metadata": {
                "extraction_method": "enhanced_hybrid_legal_extraction",
                "total_files_discovered": 0,
                "successfully_processed": 0,
                "processing_timestamp": datetime.now().isoformat()
            }
        } 