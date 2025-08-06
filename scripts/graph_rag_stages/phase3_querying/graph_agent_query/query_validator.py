"""
Validates queries to prevent hardcoded dates and other issues.
"""

import re
import logging
from datetime import datetime

log = logging.getLogger(__name__)


class QueryValidator:
    """Validates and fixes Gremlin queries before execution."""
    
    @staticmethod
    def validate_no_hardcoded_dates(query: str, original_user_query: str) -> tuple[bool, str]:
        """
        Check if query has hardcoded dates that weren't in the user's query.
        Returns (is_valid, error_message)
        """
        
        # Find all year patterns in the query (4-digit numbers that could be years)
        years_in_query = re.findall(r'\b(20\d{2}|19\d{2})\b', query)
        
        if years_in_query:
            # Check if these years were mentioned by the user
            years_in_user_query = re.findall(r'\b(20\d{2}|19\d{2})\b', original_user_query)
            
            # Find years that are in query but not in user's original question
            hardcoded_years = set(years_in_query) - set(years_in_user_query)
            
            if hardcoded_years:
                log.warning(f"⚠️ Query contains hardcoded years not in user query: {hardcoded_years}")
                return False, f"Query hardcodes years {hardcoded_years} not mentioned by user"
        
        # Check for common hardcoded date patterns
        hardcoded_patterns = [
            r"gte\('20\d{2}-\d{2}-\d{2}'\)",  # gte('2023-01-01')
            r"lte\('20\d{2}-\d{2}-\d{2}'\)",  # lte('2023-12-31')
            r"between\(.+20\d{2}.+\)",         # between with years
        ]
        
        for pattern in hardcoded_patterns:
            if re.search(pattern, query):
                # Check if this was requested by user
                if not re.search(r'\b20\d{2}\b', original_user_query):
                    log.warning(f"⚠️ Query contains hardcoded date range pattern: {pattern}")
                    return False, "Query contains hardcoded date range not requested by user"
        
        return True, ""
    
    @staticmethod
    def fix_query(query: str) -> str:
        """Fix common issues in queries."""
        
        # Fix desc/asc to decr/incr for Cosmos DB
        query = query.replace(", desc)", ", decr)")
        query = query.replace(",desc)", ",decr)")
        query = query.replace(", asc)", ", incr)")
        query = query.replace(",asc)", ",incr)")
        
        # Ensure query starts with g.
        if not query.startswith("g."):
            query = "g." + query
        
        return query
    
    @staticmethod
    def validate_and_fix(query: str, original_user_query: str) -> tuple[str, bool]:
        """
        Validate and fix a query.
        Returns (fixed_query, is_valid)
        """
        
        # Check for hardcoded dates
        is_valid, error = QueryValidator.validate_no_hardcoded_dates(query, original_user_query)
        
        if not is_valid:
            log.error(f"❌ Query validation failed: {error}")
            log.error(f"   Query: {query}")
            log.error(f"   User asked: {original_user_query}")
            # Don't execute queries with hardcoded dates
            return "", False
        
        # Fix syntax issues
        fixed_query = QueryValidator.fix_query(query)
        
        return fixed_query, True