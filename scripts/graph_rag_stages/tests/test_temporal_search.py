"""
Test cases for temporal search functionality.
"""

import pytest
import asyncio
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from scripts.graph_rag_stages.common.temporal_utils import TemporalParser, TemporalIndex
from scripts.graph_rag_stages.phase2_building.local_graph_builder import GraphBuilder
from scripts.graph_rag_stages.simple_ner.simple_query_engine import SimpleNERQueryEngine


class TestTemporalParser:
    """Test the TemporalParser utility."""
    
    def test_parse_standard_formats(self):
        """Test parsing of standard date formats."""
        test_cases = [
            ("01.09.2024", "2024-01-09"),
            ("01-09-2024", "2024-01-09"),
            ("2024-01-09", "2024-01-09"),
            ("January 9, 2024", "2024-01-09"),
            ("Jan 9, 2024", "2024-01-09"),
            ("01/09/2024", "2024-01-09"),
        ]
        
        for input_date, expected in test_cases:
            parsed = TemporalParser.parse_date(input_date)
            assert parsed is not None, f"Failed to parse {input_date}"
            assert parsed.strftime('%Y-%m-%d') == expected, f"Wrong result for {input_date}"
    
    def test_parse_relative_dates(self):
        """Test parsing of relative date expressions."""
        # Test basic relative dates
        today = date.today()
        
        # Today
        result = TemporalParser.parse_relative_date("today")
        assert result == today
        
        # Yesterday
        result = TemporalParser.parse_relative_date("yesterday")
        assert result == today - timedelta(days=1)
        
        # N days ago
        result = TemporalParser.parse_relative_date("5 days ago")
        assert result == today - timedelta(days=5)
        
        # Last month (approximate test due to month variability)
        result = TemporalParser.parse_relative_date("last month")
        assert result.year == today.year or result.year == today.year - 1
        assert abs((today - result).days - 30) < 5  # Within 5 days of 30
    
    def test_parse_quarters(self):
        """Test parsing of quarter expressions."""
        test_cases = [
            ("Q1 2024", ("2024-01-01", "2024-03-31")),
            ("Q2 2024", ("2024-04-01", "2024-06-30")),
            ("Q3 2024", ("2024-07-01", "2024-09-30")),
            ("Q4 2024", ("2024-10-01", "2024-12-31")),
            ("first quarter 2024", ("2024-01-01", "2024-03-31")),
        ]
        
        for input_text, expected in test_cases:
            result = TemporalParser.parse_quarter(input_text)
            assert result == expected, f"Wrong result for {input_text}"
    
    def test_extract_date_range(self):
        """Test extracting date ranges from text."""
        test_cases = [
            ("from January to March 2024", ("2024-01-01", "2024-03-31")),
            ("between 01/01/2024 and 01/31/2024", ("2024-01-01", "2024-01-31")),
            ("Q1 2024", ("2024-01-01", "2024-03-31")),
            ("January 2024", ("2024-01-01", "2024-01-31")),
            ("2024", ("2024-01-01", "2024-12-31")),
        ]
        
        for input_text, expected in test_cases:
            result = TemporalParser.extract_date_range(input_text)
            assert result == expected, f"Wrong result for {input_text}"


class TestTemporalIndex:
    """Test the TemporalIndex functionality."""
    
    def test_index_operations(self):
        """Test adding and retrieving from temporal index."""
        index = TemporalIndex()
        
        # Add some nodes
        index.add_node("meeting-1", "2024-01-09")
        index.add_node("meeting-2", "2024-01-23")
        index.add_node("meeting-3", "2024-02-15")
        index.add_node("meeting-4", "2024-04-10")
        
        # Test date range query
        nodes = index.get_nodes_in_range("2024-01-01", "2024-01-31")
        assert len(nodes) == 2
        assert "meeting-1" in nodes
        assert "meeting-2" in nodes
        
        # Test quarterly query
        nodes = index.get_nodes_by_quarter(2024, 1)
        assert len(nodes) == 3  # Jan, Jan, Feb
        
        # Test yearly query
        nodes = index.get_nodes_by_year(2024)
        assert len(nodes) == 4


class TestGraphBuilderTemporal:
    """Test temporal functionality in GraphBuilder."""
    
    @pytest.fixture
    def graph_builder(self, tmp_path):
        """Create a test graph builder."""
        return GraphBuilder(output_dir=tmp_path)
    
    def test_temporal_query_methods(self, graph_builder):
        """Test the temporal query methods."""
        # Add some test nodes
        from scripts.graph_rag_stages.phase2_building.local_graph_builder import MeetingProperties
        
        meetings = [
            ("meeting-1", "2024-01-09"),
            ("meeting-2", "2024-01-23"),
            ("meeting-3", "2024-02-15"),
            ("meeting-4", "2024-04-10"),
        ]
        
        for meeting_id, date in meetings:
            props = MeetingProperties(
                node_id=meeting_id,
                name=f"Meeting {date}",
                meeting_date=date
            )
            graph_builder.add_node_safe(props)
        
        # Test date range query
        results = graph_builder.query_by_date_range("2024-01-01", "2024-01-31")
        assert len(results) == 2
        
        # Test relative date query
        results = graph_builder.query_by_relative_date("Q1 2024")
        assert len(results) == 3
        
        # Test temporal progression
        progression = graph_builder.get_temporal_progression("meeting", "2024-01-01", "2024-12-31")
        assert len(progression) == 4
        assert progression[0]['date'] < progression[1]['date']  # Sorted by date


@pytest.mark.asyncio
class TestSimpleNERTemporalQueries:
    """Test temporal query handling in Simple NER engine."""
    
    async def test_temporal_query_analysis(self):
        """Test that temporal queries are properly analyzed."""
        # This would require mocking the Azure OpenAI client
        # Example structure:
        
        test_queries = [
            "Show me all documents from Q1 2024",
            "What happened last month?",
            "Find agenda items from January to March 2024",
            "List all resolutions from 2014",
            "What was discussed 30 days ago?",
        ]
        
        # Would need to mock the OpenAI response and test analysis
        pass


def run_tests():
    """Run all tests."""
    pytest.main([__file__, "-v"])


if __name__ == "__main__":
    run_tests() 