#!/usr/bin/env python3
"""
Date standardizer for consistent date formats across entity properties.
Converts various date formats to ISO 8601 (YYYY-MM-DD) while preserving specific formats.
"""

import re
from typing import Optional
import logging

log = logging.getLogger(__name__)

class DateStandardizer:
    """Standardizes date formats across entity properties."""
    
    # These formats are handled by specific logic below - not preserved as-is
    ADMINISTRATIVE_ID_FIELDS = {
        'ordinanceNumber',
        'resolutionNumber', 
        'policyNumber',
        'documentNumber',
        'contractNumber',
        'applicationNumber'
    }
    
    @staticmethod
    def normalize_to_iso_date(date_str: Optional[str], field_name: Optional[str] = None) -> Optional[str]:
        """
        Convert date strings to YYYY-MM-DD format (ISO 8601).
        
        For actual date fields: converts YYYY-MM to YYYY-MM-01 (first of month).
        For administrative fields: preserves YYYY-MM format (ordinance/resolution numbers).
        Converts DD.MM.YYYY and other formats to YYYY-MM-DD.
        
        Args:
            date_str: Input date string in various formats
            field_name: The entity field name (to distinguish dates from administrative IDs)
            
        Returns:
            Standardized date string or None if unparseable
        """
        if not date_str:
            return None
            
        date_str = str(date_str).strip()
        if not date_str:
            return None
        
        # Check if this is an administrative ID field that should preserve YYYY-MM
        if field_name and field_name in DateStandardizer.ADMINISTRATIVE_ID_FIELDS:
            # Preserve YYYY-MM for ordinance/resolution numbers
            if re.match(r'^\d{4}-\d{2}$', date_str):
                log.debug(f"Preserving administrative ID format: {field_name}={date_str}")
                return date_str
        
        # Already in ISO format (YYYY-MM-DD)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        # Convert YYYY-MM -> YYYY-MM-01 (first of month) for actual date fields
        if re.match(r'^\d{4}-\d{2}$', date_str):
            standardized = f"{date_str}-01"
            log.debug(f"Converted partial date YYYY-MM: {date_str} -> {standardized}")
            return standardized
        
        # Convert MM.DD.YYYY -> YYYY-MM-DD (American format - consistent with taxonomy/NER)
        mm_dd_yyyy = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$', date_str)
        if mm_dd_yyyy:
            month, day, year = mm_dd_yyyy.groups()
            standardized = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            log.debug(f"Converted MM.DD.YYYY: {date_str} -> {standardized}")
            return standardized
        
        # Convert MM/DD/YYYY -> YYYY-MM-DD (American format - consistent with taxonomy/NER)
        mm_slash_dd_yyyy = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', date_str)
        if mm_slash_dd_yyyy:
            month, day, year = mm_slash_dd_yyyy.groups()
            standardized = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            log.debug(f"Converted MM/DD/YYYY: {date_str} -> {standardized}")
            return standardized
        
        # Note: MM/DD/YYYY format already handled above - this was duplicate logic
        
        # Convert MM-DD-YYYY -> YYYY-MM-DD (American format - consistent with taxonomy/NER)
        mm_dash_dd_yyyy = re.match(r'^(\d{1,2})-(\d{1,2})-(\d{4})$', date_str)
        if mm_dash_dd_yyyy:
            month, day, year = mm_dash_dd_yyyy.groups()
            standardized = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            log.debug(f"Converted MM-DD-YYYY: {date_str} -> {standardized}")
            return standardized
        
        # Convert YYYY_MM_DD -> YYYY-MM-DD
        yyyy_underscore = re.match(r'^(\d{4})_(\d{1,2})_(\d{1,2})$', date_str)
        if yyyy_underscore:
            year, month, day = yyyy_underscore.groups()
            standardized = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            log.debug(f"Converted YYYY_MM_DD: {date_str} -> {standardized}")
            return standardized
        
        # Convert MM_DD_YYYY -> YYYY-MM-DD (American format - consistent with taxonomy/NER)
        mm_underscore_dd_yyyy = re.match(r'^(\d{1,2})_(\d{1,2})_(\d{4})$', date_str)
        if mm_underscore_dd_yyyy:
            month, day, year = mm_underscore_dd_yyyy.groups()
            standardized = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            log.debug(f"Converted MM_DD_YYYY: {date_str} -> {standardized}")
            return standardized
        
        # Convert YYYYMMDD -> YYYY-MM-DD
        yyyymmdd = re.match(r'^(\d{4})(\d{2})(\d{2})$', date_str)
        if yyyymmdd:
            year, month, day = yyyymmdd.groups()
            standardized = f"{year}-{month}-{day}"
            log.debug(f"Converted YYYYMMDD: {date_str} -> {standardized}")
            return standardized
        
        # Handle two-digit years (MM.DD.YY -> YYYY-MM-DD, assuming 20XX, American format)
        mm_dd_yy = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{2})$', date_str)
        if mm_dd_yy:
            month, day, year = mm_dd_yy.groups()
            full_year = f"20{year}"
            standardized = f"{full_year}-{month.zfill(2)}-{day.zfill(2)}"
            log.debug(f"Converted MM.DD.YY: {date_str} -> {standardized}")
            return standardized
        
        # Log unrecognized formats but don't fail
        log.warning(f"Unrecognized date format, preserving as-is: {date_str}")
        return date_str
    
    @staticmethod
    def get_date_fields() -> set:
        """Return set of entity property names that contain dates."""
        return {
            'dateTime',
            'meetingDate', 
            'issueDate',
            'effectiveDate',
            'startDate',
            'endDate',
            'expirationDate',
            'termStart',
            'termEnd',
            'appointmentDate',
            'confirmationDate'
        }
    
    @staticmethod
    def standardize_entity_dates(entity: dict) -> dict:
        """
        Standardize all date fields in an entity.
        
        Args:
            entity: Entity dictionary
            
        Returns:
            Entity with standardized dates (modifies in place)
        """
        date_fields = DateStandardizer.get_date_fields()
        
        for field_name, value in list(entity.items()):
            if field_name in date_fields and value is not None:
                # Pass field_name to distinguish dates from administrative IDs
                standardized = DateStandardizer.normalize_to_iso_date(value, field_name)
                if standardized != value:
                    entity[field_name] = standardized
                    log.debug(f"Standardized {field_name}: {value} -> {standardized}")
        
        return entity

# Convenience function for backward compatibility
def normalize_date(date_str: Optional[str], field_name: Optional[str] = None) -> Optional[str]:
    """Convenience function for date normalization."""
    return DateStandardizer.normalize_to_iso_date(date_str, field_name)
