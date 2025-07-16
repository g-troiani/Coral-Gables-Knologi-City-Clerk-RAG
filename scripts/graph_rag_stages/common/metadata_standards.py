from pathlib import Path
from typing import Dict, Any, Optional
import re

class MetadataStandards:
    """Ensures consistent metadata across all file types."""
    
    # Standard metadata fields
    SOURCE_FILE_NAME = "Source_File_Name"
    SOURCE_FILE_PATH = "Source_File_Path"
    DOCUMENT_TYPE = "Document_Type"
    MEETING_DATE = "Meeting_Date"
    
    @staticmethod
    def classify_document(filename: str, title: str = "") -> str:
        """
        Classify document type based on filename and title content.
        
        Args:
            filename: The filename to analyze
            title: Optional title content to help with classification
            
        Returns:
            Document type classification string
        """
        filename_lower = filename.lower()
        title_lower = title.lower() if title else ""
        
        # Check filename patterns first
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