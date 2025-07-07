"""
Temporal utilities for date parsing, range handling, and relative date interpretation.
"""

import re
from datetime import datetime, timedelta, date
from typing import Tuple, Optional, List, Union, Set
from dateutil import parser
from dateutil.relativedelta import relativedelta
import calendar

class TemporalParser:
    """Handles various date formats and relative date expressions."""
    
    # Common date formats in the documents
    DATE_FORMATS = [
        '%m.%d.%Y',      # 01.09.2024
        '%m-%d-%Y',      # 01-09-2024
        '%Y-%m-%d',      # 2024-01-09
        '%B %d, %Y',     # January 9, 2024
        '%b %d, %Y',     # Jan 9, 2024
        '%m/%d/%Y',      # 01/09/2024
        '%Y%m%d',        # 20240109
    ]
    
    # Relative date patterns
    RELATIVE_PATTERNS = {
        r'today': lambda: date.today(),
        r'yesterday': lambda: date.today() - timedelta(days=1),
        r'tomorrow': lambda: date.today() + timedelta(days=1),
        r'last\s+week': lambda: date.today() - timedelta(weeks=1),
        r'next\s+week': lambda: date.today() + timedelta(weeks=1),
        r'last\s+month': lambda: date.today() - relativedelta(months=1),
        r'next\s+month': lambda: date.today() + relativedelta(months=1),
        r'last\s+year': lambda: date.today() - relativedelta(years=1),
        r'next\s+year': lambda: date.today() + relativedelta(years=1),
        r'(\d+)\s+days?\s+ago': lambda m: date.today() - timedelta(days=int(m.group(1))),
        r'(\d+)\s+weeks?\s+ago': lambda m: date.today() - timedelta(weeks=int(m.group(1))),
        r'(\d+)\s+months?\s+ago': lambda m: date.today() - relativedelta(months=int(m.group(1))),
        r'(\d+)\s+years?\s+ago': lambda m: date.today() - relativedelta(years=int(m.group(1))),
    }
    
    # Quarter patterns
    QUARTER_PATTERNS = {
        r'Q1\s+(\d{4})': lambda year: (f"{year}-01-01", f"{year}-03-31"),
        r'Q2\s+(\d{4})': lambda year: (f"{year}-04-01", f"{year}-06-30"),
        r'Q3\s+(\d{4})': lambda year: (f"{year}-07-01", f"{year}-09-30"),
        r'Q4\s+(\d{4})': lambda year: (f"{year}-10-01", f"{year}-12-31"),
        r'first\s+quarter\s+(\d{4})': lambda year: (f"{year}-01-01", f"{year}-03-31"),
        r'second\s+quarter\s+(\d{4})': lambda year: (f"{year}-04-01", f"{year}-06-30"),
        r'third\s+quarter\s+(\d{4})': lambda year: (f"{year}-07-01", f"{year}-09-30"),
        r'fourth\s+quarter\s+(\d{4})': lambda year: (f"{year}-10-01", f"{year}-12-31"),
    }
    
    @classmethod
    def parse_date(cls, date_str: str) -> Optional[datetime]:
        """Parse a date string into datetime object."""
        if not date_str:
            return None
            
        date_str = date_str.strip()
        
        # Try standard formats first
        for fmt in cls.DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Try dateutil parser as fallback
        try:
            return parser.parse(date_str, fuzzy=False)
        except:
            return None
    
    @classmethod
    def parse_relative_date(cls, text: str) -> Optional[date]:
        """Parse relative date expressions like 'last month', '3 days ago'."""
        text = text.lower().strip()
        
        for pattern, func in cls.RELATIVE_PATTERNS.items():
            match = re.search(pattern, text)
            if match:
                try:
                    if match.groups():
                        return func(match)
                    else:
                        return func()
                except:
                    continue
        
        return None
    
    @classmethod
    def parse_quarter(cls, text: str) -> Optional[Tuple[str, str]]:
        """Parse quarter expressions like 'Q1 2024' into date range."""
        text = text.strip()
        
        for pattern, func in cls.QUARTER_PATTERNS.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                year = match.group(1)
                return func(year)
        
        return None
    
    @classmethod
    def extract_date_range(cls, text: str) -> Optional[Tuple[str, str]]:
        """Extract date range from text like 'from January to March 2024'."""
        # Pattern for "from X to Y" or "between X and Y"
        range_patterns = [
            r'from\s+(.+?)\s+to\s+(.+)',
            r'between\s+(.+?)\s+and\s+(.+)',
            r'(\w+)\s*-\s*(\w+)\s+(\d{4})',  # Jan-Mar 2024
        ]
        
        for pattern in range_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if len(match.groups()) == 3:  # Month range with year
                    start_month = match.group(1)
                    end_month = match.group(2)
                    year = match.group(3)
                    
                    try:
                        start_date = parser.parse(f"{start_month} 1, {year}")
                        end_date = parser.parse(f"{end_month} 1, {year}")
                        # Get last day of end month
                        last_day = calendar.monthrange(end_date.year, end_date.month)[1]
                        end_date = end_date.replace(day=last_day)
                        
                        return (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                    except:
                        continue
                else:
                    start_str = match.group(1)
                    end_str = match.group(2)
                    
                    start_date = cls.parse_date(start_str) or cls.parse_relative_date(start_str)
                    end_date = cls.parse_date(end_str) or cls.parse_relative_date(end_str)
                    
                    if start_date and end_date:
                        if isinstance(start_date, date):
                            start_date = datetime.combine(start_date, datetime.min.time())
                        if isinstance(end_date, date):
                            end_date = datetime.combine(end_date, datetime.min.time())
                        
                        return (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        # Check for quarter
        quarter_range = cls.parse_quarter(text)
        if quarter_range:
            return quarter_range
        
        # Check for month/year
        month_year_match = re.search(r'(\w+)\s+(\d{4})', text)
        if month_year_match:
            try:
                month_str = month_year_match.group(1)
                year = month_year_match.group(2)
                start_date = parser.parse(f"{month_str} 1, {year}")
                last_day = calendar.monthrange(start_date.year, start_date.month)[1]
                end_date = start_date.replace(day=last_day)
                return (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            except:
                pass
        
        # Check for year only
        year_match = re.search(r'\b(20\d{2})\b', text)
        if year_match:
            year = year_match.group(1)
            return (f"{year}-01-01", f"{year}-12-31")
        
        return None
    
    @classmethod
    def normalize_date(cls, date_input: Union[str, datetime, date]) -> Optional[str]:
        """Normalize any date input to YYYY-MM-DD format."""
        if isinstance(date_input, datetime):
            return date_input.strftime('%Y-%m-%d')
        elif isinstance(date_input, date):
            return date_input.strftime('%Y-%m-%d')
        elif isinstance(date_input, str):
            parsed = cls.parse_date(date_input)
            if parsed:
                return parsed.strftime('%Y-%m-%d')
        
        return None


class TemporalIndex:
    """Maintains temporal indices for fast date-based lookups."""
    
    def __init__(self):
        self.date_to_nodes = {}  # date -> set of node IDs
        self.year_to_nodes = {}  # year -> set of node IDs
        self.month_to_nodes = {}  # year-month -> set of node IDs
        self.quarter_to_nodes = {}  # year-Q# -> set of node IDs
    
    def add_node(self, node_id: str, date_str: str) -> None:
        """Add a node to temporal indices."""
        parsed_date = TemporalParser.parse_date(date_str)
        if not parsed_date:
            return
        
        # Daily index
        date_key = parsed_date.strftime('%Y-%m-%d')
        if date_key not in self.date_to_nodes:
            self.date_to_nodes[date_key] = set()
        self.date_to_nodes[date_key].add(node_id)
        
        # Yearly index
        year_key = str(parsed_date.year)
        if year_key not in self.year_to_nodes:
            self.year_to_nodes[year_key] = set()
        self.year_to_nodes[year_key].add(node_id)
        
        # Monthly index
        month_key = parsed_date.strftime('%Y-%m')
        if month_key not in self.month_to_nodes:
            self.month_to_nodes[month_key] = set()
        self.month_to_nodes[month_key].add(node_id)
        
        # Quarterly index
        quarter = (parsed_date.month - 1) // 3 + 1
        quarter_key = f"{parsed_date.year}-Q{quarter}"
        if quarter_key not in self.quarter_to_nodes:
            self.quarter_to_nodes[quarter_key] = set()
        self.quarter_to_nodes[quarter_key].add(node_id)
    
    def get_nodes_in_range(self, start_date: str, end_date: str) -> Set[str]:
        """Get all nodes within a date range."""
        nodes = set()
        
        start = TemporalParser.parse_date(start_date)
        end = TemporalParser.parse_date(end_date)
        
        if not start or not end:
            return nodes
        
        # Use daily index for small ranges (< 90 days)
        if (end - start).days < 90:
            current = start
            while current <= end:
                date_key = current.strftime('%Y-%m-%d')
                if date_key in self.date_to_nodes:
                    nodes.update(self.date_to_nodes[date_key])
                current += timedelta(days=1)
        else:
            # Use monthly index for larger ranges
            current = start.replace(day=1)
            while current <= end:
                month_key = current.strftime('%Y-%m')
                if month_key in self.month_to_nodes:
                    # Filter nodes to exact range
                    for node_id in self.month_to_nodes[month_key]:
                        # Would need to check actual node date here
                        nodes.add(node_id)
                
                # Move to next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
        
        return nodes
    
    def get_nodes_by_quarter(self, year: int, quarter: int) -> Set[str]:
        """Get all nodes in a specific quarter."""
        quarter_key = f"{year}-Q{quarter}"
        return self.quarter_to_nodes.get(quarter_key, set())
    
    def get_nodes_by_year(self, year: int) -> Set[str]:
        """Get all nodes in a specific year."""
        return self.year_to_nodes.get(str(year), set())


def natural_item_sort_key(item_code: str) -> Tuple[str, int]:
    """
    Generate a sort key for agenda item codes to enable natural sorting.
    
    Examples:
        E-1, E-2, E-10, F-1, F-2
    
    Args:
        item_code: Agenda item code like "E-1", "F-10", etc.
        
    Returns:
        Tuple of (letter_part, number_part) for sorting
    """
    if not item_code:
        return ("", 0)
    
    # Extract letter and number parts
    match = re.match(r'^([A-Z]+)-?(\d+)$', str(item_code).upper())
    if match:
        letter_part = match.group(1)
        number_part = int(match.group(2))
        return (letter_part, number_part)
    
    # Fallback: treat as string
    return (str(item_code), 0) 