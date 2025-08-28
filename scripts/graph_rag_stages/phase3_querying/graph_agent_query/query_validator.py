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
        
        # Check for temporal range expressions in user query
        temporal_patterns = [
            r'\bsince\s+(\d{4})\b',         # "since 2010"
            r'\bfrom\s+(\d{4})\b',          # "from 2010" 
            r'\bafter\s+(\d{4})\b',         # "after 2010"
            r'\bbefore\s+(\d{4})\b',        # "before 2020"
            r'\buntil\s+(\d{4})\b',         # "until 2020"
            r'\bthrough\s+(\d{4})\b',       # "through 2020"
            r'\bfrom\s+(\d{4})\s+to\s+(\d{4})\b',  # "from 2010 to 2020"
            r'\bbetween\s+(\d{4})\s+and\s+(\d{4})\b', # "between 2010 and 2020"
        ]
        
        temporal_expression_found = False
        for pattern in temporal_patterns:
            if re.search(pattern, original_user_query, re.IGNORECASE):
                temporal_expression_found = True
                log.info(f"✅ Temporal expression detected: {pattern} in '{original_user_query}'")
                break
        
        # If temporal expression found, allow expanded year ranges in query
        if temporal_expression_found:
            log.info("✅ Temporal range query - allowing expanded year ranges in generated query")
            return True, ""
        
        # Find all year patterns in the query (4-digit numbers that could be years)
        years_in_query = re.findall(r'\b(20\d{2}|19\d{2})\b', query)
        
        if years_in_query:
            # Check if these years were mentioned by the user (including 2-digit year formats)
            years_in_user_query = re.findall(r'\b(20\d{2}|19\d{2})\b', original_user_query)
            
            # Also check for 2-digit years that could expand to 4-digit (e.g., "24" -> "2024")
            two_digit_years = re.findall(r'\b(\d{2})\b', original_user_query)
            expanded_years = []
            for year_2d in two_digit_years:
                if 0 <= int(year_2d) <= 99:  # Valid 2-digit year
                    if int(year_2d) <= 30:  # Assume 00-30 means 2000-2030
                        expanded_years.append(f"20{year_2d}")
                    else:  # 31-99 means 1931-1999
                        expanded_years.append(f"19{year_2d}")
            
            # Also check for date patterns that contain years (e.g., "01.09.24" contains "24")
            date_patterns = re.findall(r'\b\d{1,2}[.\-/]\d{1,2}[.\-/](\d{2,4})\b', original_user_query)
            for year in date_patterns:
                if len(year) == 2:  # 2-digit year
                    if int(year) <= 30:
                        expanded_years.append(f"20{year}")
                    else:
                        expanded_years.append(f"19{year}")
                else:  # 4-digit year
                    expanded_years.append(year)
            
            # Combine all years the user mentioned (explicit 4-digit + expanded 2-digit)
            all_user_years = set(years_in_user_query + expanded_years)
            
            # Find years that are in query but not in user's original question
            hardcoded_years = set(years_in_query) - all_user_years
            
            if hardcoded_years:
                log.warning(f"⚠️ Query contains hardcoded years not in user query: {hardcoded_years}")
                log.info(f"   User mentioned years: {all_user_years}")
                log.info(f"   Query contains years: {set(years_in_query)}")
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