from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import re

class MetadataStandards:
    """Ensures consistent metadata across all file types."""
    
    # Standard metadata fields
    SOURCE_FILE_NAME = "Source_File_Name"
    SOURCE_FILE_PATH = "Source_File_Path"
    DOCUMENT_TYPE = "Document_Type"
    MEETING_DATE = "Meeting_Date"
    
    # Required fields for different document types
    REQUIRED_FIELDS = {
        "agenda": ["meeting_date", "Source_File_Name"],
        "ordinance": ["document_number", "Source_File_Name"],
        "resolution": ["document_number", "Source_File_Name"],
        "verbatim_transcript": ["meeting_date", "Source_File_Name"],
        "default": ["Source_File_Name"]
    }
    
    @staticmethod
    def validate_metadata(metadata: Dict[str, Any], document_type: str = "default") -> Tuple[bool, List[str]]:
        """
        Validate that metadata contains required fields.
        
        Args:
            metadata: The metadata dictionary to validate
            document_type: The type of document to validate against
            
        Returns:
            Tuple of (is_valid, list_of_missing_fields)
        """
        missing_fields = []
        
        # Get required fields for this document type
        doc_type_lower = document_type.lower() if document_type else "default"
        required = MetadataStandards.REQUIRED_FIELDS.get(doc_type_lower, 
                                                         MetadataStandards.REQUIRED_FIELDS["default"])
        
        # Check each required field
        for field in required:
            # Check multiple possible field names for backward compatibility
            field_found = False
            
            if field == "Source_File_Name":
                # Check various possible field names
                for alt_field in ["Source_File_Name", "source_file", "Source_File", "filename"]:
                    if alt_field in metadata and metadata[alt_field]:
                        field_found = True
                        break
            elif field == "Source_File_Path":
                # Check various possible field names
                for alt_field in ["Source_File_Path", "file_path", "File_Path", "path"]:
                    if alt_field in metadata and metadata[alt_field]:
                        field_found = True
                        break
            elif field == "meeting_date":
                # Check various possible field names
                for alt_field in ["meeting_date", "Meeting_Date", "meetingDate"]:
                    if alt_field in metadata and metadata[alt_field]:
                        field_found = True
                        break
            elif field == "document_number":
                # Check various possible field names
                for alt_field in ["document_number", "Document_Number", "doc_number"]:
                    if alt_field in metadata and metadata[alt_field]:
                        field_found = True
                        break
            else:
                # For other fields, just check if it exists
                if field in metadata and metadata[field]:
                    field_found = True
            
            if not field_found:
                missing_fields.append(field)
        
        is_valid = len(missing_fields) == 0
        return is_valid, missing_fields
    
    @staticmethod
    def classify_document(filename: str, title: str = "", file_path: str = "") -> str:
        """
        Classify document type based on filename, title content, and file path.
        
        Args:
            filename: The filename to analyze
            title: Optional title content to help with classification
            file_path: Optional full file path to check folder structure
            
        Returns:
            Document type classification string
        """
        filename_lower = filename.lower()
        title_lower = title.lower() if title else ""
        file_path_lower = file_path.lower() if file_path else ""
        
        # Check file path for folder structure first (most reliable)
        if "/resolutions/" in file_path_lower or "\\resolutions\\" in file_path_lower:
            return "resolution"
        elif "/ordinances/" in file_path_lower or "\\ordinances\\" in file_path_lower:
            return "ordinance"
        
        # Check filename patterns
        if "ordinance" in filename_lower or "ord" in filename_lower:
            return "ordinance"
        elif "resolution" in filename_lower or "res" in filename_lower:
            return "resolution"
        elif "agenda" in filename_lower:
            return "agenda"
        elif "verbatim" in filename_lower or "transcript" in filename_lower:
            return "verbatim_transcript"
        elif "minutes" in filename_lower:
            return "meeting_minutes"
        elif "proclamation" in filename_lower:
            return "proclamation"
        elif "contract" in filename_lower:
            return "contract"
        
        # Check title content
        if title:
            if "ordinance" in title_lower:
                return "ordinance"
            elif "resolution" in title_lower:
                return "resolution"
            elif "proclamation" in title_lower:
                return "proclamation"
            elif "contract" in title_lower:
                return "contract"
            elif "agreement" in title_lower:
                return "agreement"
        
        # Check for special ordinance types by prefix
        special_prefixes = ["SOE", "CG", "EO", "CAO"]
        for prefix in special_prefixes:
            if filename.startswith(prefix):
                return "special_ordinance"
        
        # Check for amendment documents
        if "amendment" in filename_lower:
            return "amendment"
        
        # Default
        return "document"
    
    @staticmethod
    def standardize_metadata(data: Dict[str, Any], source_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        Standardize metadata keys and ensure both name and path exist.
        
        Args:
            data: The metadata dictionary to standardize
            source_path: Optional Path object to extract metadata from
            
        Returns:
            Standardized metadata dictionary
        """
        standardized = data.copy()
        
        # Handle source file metadata
        if source_path:
            standardized[MetadataStandards.SOURCE_FILE_NAME] = source_path.name
            standardized[MetadataStandards.SOURCE_FILE_PATH] = str(source_path)
        else:
            # Try to get from existing fields with various fallbacks
            if MetadataStandards.SOURCE_FILE_NAME not in standardized:
                # Check various possible field names
                for field in ["source_file", "Source_File", "source", "filename", "file_name"]:
                    if field in data:
                        standardized[MetadataStandards.SOURCE_FILE_NAME] = data[field]
                        break
            
            if MetadataStandards.SOURCE_FILE_PATH not in standardized:
                # Check various possible field names
                for field in ["file_path", "File_Path", "path", "full_path", "source_path"]:
                    if field in data:
                        standardized[MetadataStandards.SOURCE_FILE_PATH] = data[field]
                        break
        
        # Ensure we have both fields (with defaults if necessary)
        if MetadataStandards.SOURCE_FILE_NAME not in standardized:
            standardized[MetadataStandards.SOURCE_FILE_NAME] = "unknown"
        if MetadataStandards.SOURCE_FILE_PATH not in standardized:
            standardized[MetadataStandards.SOURCE_FILE_PATH] = "unknown"
        
        # Standardize other common fields
        if "document_type" in data and MetadataStandards.DOCUMENT_TYPE not in standardized:
            standardized[MetadataStandards.DOCUMENT_TYPE] = data["document_type"]
        if "meeting_date" in data and MetadataStandards.MEETING_DATE not in standardized:
            standardized[MetadataStandards.MEETING_DATE] = data["meeting_date"]
            
        return standardized
    
    @staticmethod
    def format_markdown_header(metadata: Dict[str, Any]) -> str:
        """
        Format metadata as markdown YAML header.
        
        Args:
            metadata: The metadata dictionary to format
            
        Returns:
            Formatted YAML header string
        """
        lines = ["---"]
        
        # Define the order of fields for consistent output
        field_order = [
            MetadataStandards.MEETING_DATE,
            MetadataStandards.DOCUMENT_TYPE,
            "Document_Number",
            "Agenda_Item",
            MetadataStandards.SOURCE_FILE_NAME,
            MetadataStandards.SOURCE_FILE_PATH
        ]
        
        # Add ordered fields first
        for field in field_order:
            if field in metadata:
                lines.append(f"- {field}: {metadata[field]}")
        
        # Add any remaining fields
        for key, value in metadata.items():
            if key not in field_order and value is not None:
                # Convert key to title case with underscores
                display_key = key.replace(" ", "_")
                lines.append(f"- {display_key}: {value}")
        
        lines.append("---")
        return "\n".join(lines)
    
    @staticmethod
    def extract_metadata_from_markdown(content: str) -> Dict[str, Any]:
        """
        Extract metadata from markdown YAML header.
        
        Args:
            content: The markdown content with YAML header
            
        Returns:
            Dictionary of extracted metadata
        """
        metadata = {}
        
        if content.startswith("---"):
            try:
                # Find the closing ---
                end_index = content.find("---", 3)
                if end_index > 0:
                    header_section = content[3:end_index].strip()
                    
                    # Parse each line
                    for line in header_section.split("\n"):
                        line = line.strip()
                        if line.startswith("- ") and ":" in line:
                            key_value = line[2:].split(":", 1)
                            if len(key_value) == 2:
                                key = key_value[0].strip()
                                value = key_value[1].strip()
                                metadata[key] = value
            except Exception:
                # If parsing fails, return empty metadata
                pass
        
        return metadata
    
    @staticmethod
    def is_legal_document(filename: str, title: str = "") -> bool:
        """
        Check if a document is a legal document (ordinance or resolution).
        
        Args:
            filename: The filename to check
            title: Optional title to check
            
        Returns:
            True if it's a legal document, False otherwise
        """
        doc_type = MetadataStandards.classify_document(filename, title)
        return doc_type in ["ordinance", "resolution", "special_ordinance"]
    
    @staticmethod
    def normalize_date_format(date_str: str) -> str:
        """
        Normalize various date formats to MM.DD.YYYY format.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            Normalized date string in MM.DD.YYYY format
        """
        if not date_str:
            return ""
        
        # Already in correct format
        if re.match(r'^\d{2}\.\d{2}\.\d{4}$', date_str):
            return date_str
        
        # Try different patterns
        patterns = [
            (r'^(\d{2})/(\d{2})/(\d{4})$', lambda m: f"{m.group(1)}.{m.group(2)}.{m.group(3)}"),
            (r'^(\d{2})-(\d{2})-(\d{4})$', lambda m: f"{m.group(1)}.{m.group(2)}.{m.group(3)}"),
            (r'^(\d{2})_(\d{2})_(\d{4})$', lambda m: f"{m.group(1)}.{m.group(2)}.{m.group(3)}"),
            (r'^(\d{4})-(\d{2})-(\d{2})$', lambda m: f"{m.group(2)}.{m.group(3)}.{m.group(1)}"),
        ]
        
        for pattern, formatter in patterns:
            match = re.match(pattern, date_str)
            if match:
                return formatter(match)
        
        return date_str  # Return as-is if no pattern matches 