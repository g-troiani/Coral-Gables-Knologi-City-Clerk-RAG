"""
Enhanced Document Linker
Links both ordinance and resolution documents to their corresponding agenda items.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import json
from datetime import datetime
import PyPDF2
from groq import Groq
import os
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger('enhanced_document_linker')


class EnhancedDocumentLinker:
    """Links ordinance and resolution documents to agenda items."""
    
    def __init__(self,
                 groq_api_key: Optional[str] = None,
                 model: str = "qwen-qwq-32b",
                 agenda_extraction_max_tokens: int = 100000):
        """Initialize the enhanced document linker."""
        self.api_key = groq_api_key or os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment")
        
        self.client = Groq(api_key=self.api_key)
        self.model = model
        self.agenda_extraction_max_tokens = agenda_extraction_max_tokens
    
    def _parse_qwen_response(self, response_text: str) -> str:
        """Parse qwen response to extract content outside thinking tags."""
        thinking_pattern = r'<thinking>.*?</thinking>'
        cleaned_text = re.sub(thinking_pattern, '', response_text, flags=re.DOTALL)
        cleaned_text = re.sub(r'<[^>]+>', '', cleaned_text)
        return cleaned_text.strip()
    
    async def link_documents_for_meeting(self, 
                                       meeting_date: str,
                                       ordinances_dir: Path,
                                       resolutions_dir: Path) -> Dict[str, List[Dict]]:
        """Find and link all documents (ordinances AND resolutions) for a specific meeting date."""
        log.info(f"🔗 Enhanced linking: documents for meeting date: {meeting_date}")
        log.info(f"📁 Ordinances directory: {ordinances_dir}")
        log.info(f"📁 Resolutions directory: {resolutions_dir}")
        
        # Create debug directory
        debug_dir = Path("city_clerk_documents/graph_json/debug")
        debug_dir.mkdir(exist_ok=True)
        
        # Convert date format: "01.09.2024" -> "01_09_2024"
        date_underscore = meeting_date.replace(".", "_")
        
        # Initialize results
        linked_documents = {
            "ordinances": [],
            "resolutions": []
        }
        
        # Process ordinances
        if ordinances_dir.exists():
            ordinance_files = self._find_matching_files(ordinances_dir, date_underscore)
            log.info(f"📄 Found {len(ordinance_files)} ordinance files")
            
            for doc_path in ordinance_files:
                doc_info = await self._process_document(doc_path, meeting_date, "ordinance")
                if doc_info:
                    linked_documents["ordinances"].append(doc_info)
        else:
            log.warning(f"⚠️  Ordinances directory not found: {ordinances_dir}")
        
        # Process resolutions - NEW LOGIC
        if resolutions_dir.exists():
            # Check for year subdirectory first
            year = meeting_date.split('.')[-1]  # Extract year from date
            year_dir = resolutions_dir / year
            
            if year_dir.exists():
                resolution_files = self._find_matching_files(year_dir, date_underscore)
                log.info(f"📄 Found {len(resolution_files)} resolution files in {year} directory")
            else:
                # Fall back to main resolutions directory
                resolution_files = self._find_matching_files(resolutions_dir, date_underscore)
                log.info(f"📄 Found {len(resolution_files)} resolution files in main directory")
            
            for doc_path in resolution_files:
                doc_info = await self._process_document(doc_path, meeting_date, "resolution")
                if doc_info:
                    linked_documents["resolutions"].append(doc_info)
        else:
            log.warning(f"⚠️  Resolutions directory not found: {resolutions_dir}")
        
        # Save enhanced linked documents info
        with open(debug_dir / "enhanced_linked_documents.json", 'w') as f:
            json.dump(linked_documents, f, indent=2)
        
        # Log summary
        total_linked = len(linked_documents['ordinances']) + len(linked_documents['resolutions'])
        log.info(f"✅ Enhanced linking complete:")
        log.info(f"   📄 Ordinances: {len(linked_documents['ordinances'])}")
        log.info(f"   📄 Resolutions: {len(linked_documents['resolutions'])}")
        log.info(f"   📄 Total linked: {total_linked}")
        
        # Save detailed report
        self._generate_linking_report(meeting_date, linked_documents, debug_dir)
        
        return linked_documents
    
    def _find_matching_files(self, directory: Path, date_pattern: str) -> List[Path]:
        """Find all PDF files matching the date pattern."""
        # Pattern: YYYY-## - MM_DD_YYYY.pdf
        pattern = f"*{date_pattern}.pdf"
        matching_files = list(directory.glob(pattern))
        
        # Also try without spaces in case filenames vary
        pattern2 = f"*{date_pattern}*.pdf"
        additional_files = [f for f in directory.glob(pattern2) if f not in matching_files]
        matching_files.extend(additional_files)
        
        # Also check for variations in date format
        # Some files might use dashes instead of underscores
        date_dash = date_pattern.replace("_", "-")
        pattern3 = f"*{date_dash}*.pdf"
        more_files = [f for f in directory.glob(pattern3) if f not in matching_files]
        matching_files.extend(more_files)
        
        return sorted(matching_files)
    
    async def _process_document(self, doc_path: Path, meeting_date: str, doc_type: str) -> Optional[Dict[str, Any]]:
        """Process a single document to extract agenda item reference."""
        try:
            # Extract document number from filename
            doc_match = re.match(r'^(\d{4}-\d{2,3})', doc_path.name)
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
            item_code = await self._extract_agenda_item_code(text, document_number, doc_type)
            
            # Extract additional metadata
            parsed_data = self._parse_document_metadata(text, doc_type)
            
            doc_info = {
                "path": str(doc_path),
                "filename": doc_path.name,
                "document_number": document_number,
                "item_code": item_code,
                "document_type": doc_type.capitalize(),
                "title": self._extract_title(text, doc_type),
                "parsed_data": parsed_data
            }
            
            log.info(f"📄 Processed {doc_type} {doc_path.name}: Item {item_code or 'NOT_FOUND'}")
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
    
    async def _extract_agenda_item_code(self, text: str, document_number: str, doc_type: str) -> Optional[str]:
        """Extract agenda item code from document text using LLM."""
        debug_dir = Path("city_clerk_documents/graph_json/debug")
        debug_dir.mkdir(exist_ok=True)
        
        # Customize prompt based on document type
        if doc_type == "resolution":
            doc_type_text = "resolution"
            typical_sections = "F items (e.g., F-1, F-2, F-3)"
        else:
            doc_type_text = "ordinance"
            typical_sections = "E items (e.g., E-1, E-2, E-3)"
        
        prompt = f"""You are analyzing a City of Coral Gables {doc_type_text} document (Document #{document_number}).

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
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": f"You are a precise data extractor for {doc_type_text} documents. Find and extract only the agenda item code. Search the ENTIRE document thoroughly."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=self.agenda_extraction_max_tokens
            )
            
            raw_response = response.choices[0].message.content.strip()
            
            # Save raw LLM response for debugging
            with open(debug_dir / f"llm_response_{doc_type}_{document_number}_raw.txt", 'w', encoding='utf-8') as f:
                f.write(raw_response)
            
            # Parse response
            result = self._parse_qwen_response(raw_response)
            
            # Save cleaned response
            with open(debug_dir / f"llm_response_{doc_type}_{document_number}_cleaned.txt", 'w', encoding='utf-8') as f:
                f.write(result)
            
            # Parse the response
            if "AGENDA_ITEM:" in result:
                # Extract just the code part, stopping at first space or newline after the code
                parts = result.split("AGENDA_ITEM:")[1].strip()
                
                # Extract just the code pattern (letter-number)
                code_match = re.match(r'^([A-Z]-?\d+)', parts)
                if code_match:
                    code = code_match.group(1)
                    if code != "NOT_FOUND":
                        code = self._normalize_item_code(code)
                        log.info(f"✅ Found agenda item code for {document_number}: {code}")
                        return code
                elif parts.startswith("NOT_FOUND"):
                    log.warning(f"❌ LLM could not find agenda item in {document_number}")
                else:
                    # Try to extract code from a messy response
                    code_pattern = r'\b([A-Z]-?\d+)\b'
                    match = re.search(code_pattern, parts)
                    if match:
                        code = self._normalize_item_code(match.group(1))
                        log.info(f"✅ Extracted agenda item code for {document_number}: {code} (from messy response)")
                        return code
                    log.error(f"❌ Could not parse item code from response: {parts[:100]}")
            else:
                log.error(f"❌ Invalid LLM response format for {document_number}: {result[:100]}")
            
            return None
            
        except Exception as e:
            log.error(f"Failed to extract agenda item for {doc_type} {document_number}: {e}")
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
        
        # Ensure we have a dash between letter and number
        code = re.sub(r'([A-Z])(\d)', r'\1-\2', code)
        
        return code
    
    def _extract_title(self, text: str, doc_type: str) -> str:
        """Extract document title from text."""
        # Look for "AN ORDINANCE" or "A RESOLUTION" pattern
        if doc_type == "resolution":
            pattern = r'(A\s+RESOLUTION[^.]+\.)'
        else:
            pattern = r'(AN?\s+ORDINANCE[^.]+\.)'
            
        title_match = re.search(pattern, text[:2000], re.IGNORECASE)
        if title_match:
            return title_match.group(1).strip()
        
        # Fallback to first substantive line
        lines = text.split('\n')
        for line in lines[:20]:
            if len(line) > 20 and not line.isdigit():
                return line.strip()[:200]
        
        return f"Untitled {doc_type.capitalize()}"
    
    def _parse_document_metadata(self, text: str, doc_type: str) -> Dict[str, Any]:
        """Parse additional metadata from document."""
        metadata = {
            "document_type": doc_type
        }
        
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
        
        # Resolution-specific metadata
        if doc_type == "resolution":
            # Look for resolution-specific patterns
            purpose_match = re.search(r'(?:WHEREAS|PURPOSE)[:\s]+([^.]+)', text, re.IGNORECASE)
            if purpose_match:
                metadata["purpose"] = purpose_match.group(1).strip()
        
        return metadata
    
    def _generate_linking_report(self, meeting_date: str, linked_documents: Dict, debug_dir: Path):
        """Generate a detailed report of the linking process."""
        report = {
            "meeting_date": meeting_date,
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_ordinances": len(linked_documents["ordinances"]),
                "total_resolutions": len(linked_documents["resolutions"]),
                "ordinances_with_items": len([d for d in linked_documents["ordinances"] if d.get("item_code")]),
                "resolutions_with_items": len([d for d in linked_documents["resolutions"] if d.get("item_code")]),
                "unlinked_ordinances": len([d for d in linked_documents["ordinances"] if not d.get("item_code")]),
                "unlinked_resolutions": len([d for d in linked_documents["resolutions"] if not d.get("item_code")])
            },
            "details": {
                "ordinances": [
                    {
                        "document_number": d["document_number"],
                        "item_code": d.get("item_code", "NOT_FOUND"),
                        "title": d.get("title", "")[:100]
                    }
                    for d in linked_documents["ordinances"]
                ],
                "resolutions": [
                    {
                        "document_number": d["document_number"],
                        "item_code": d.get("item_code", "NOT_FOUND"),
                        "title": d.get("title", "")[:100]
                    }
                    for d in linked_documents["resolutions"]
                ]
            }
        }
        
        # FIXED: Use double quotes for f-string
        report_filename = f"linking_report_{meeting_date.replace('.', '_')}.json"
        report_path = debug_dir / report_filename
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        log.info(f"📊 Linking report saved to: {report_path}")
    
    # Add backward compatibility method
    async def link_documents_for_meeting_legacy(self, 
                                               meeting_date: str,
                                               documents_dir: Path) -> Dict[str, List[Dict]]:
        """Legacy method for backward compatibility - ordinances only."""
        return await self.link_documents_for_meeting(
            meeting_date,
            documents_dir,  # Ordinances directory
            Path("dummy")   # No resolutions directory
        ) 