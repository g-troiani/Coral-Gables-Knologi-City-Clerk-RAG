#!/usr/bin/env python3
"""
Enhanced Hierarchical Document Extractor
Builds on sophisticated OCR + LLM pipeline for superior hierarchical relationships
"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import re
from datetime import datetime
from dataclasses import dataclass, asdict

log = logging.getLogger(__name__)

@dataclass
class HierarchicalNode:
    """Represents a node in the document hierarchy."""
    node_id: str
    node_type: str
    title: str
    level: int
    parent_id: Optional[str] = None
    children: List[str] = None
    properties: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
        if self.properties is None:
            self.properties = {}

# Remove HierarchicalRelationship dataclass - using dictionaries for relationships instead

class EnhancedHierarchicalExtractor:
    """Enhanced extractor for hierarchical document structures."""
    
    def __init__(self):
        self.nodes: Dict[str, HierarchicalNode] = {}
        self.relationships: List[Dict[str, Any]] = []
        
        # Hierarchy levels for different document types
        self.hierarchy_levels = {
            'MEETING': 0,
            'SECTION': 1, 
            'AGENDA_ITEM': 2,
            'ORDINANCE': 3,
            'RESOLUTION': 3,
            'TRANSCRIPT': 3,
            'DOCUMENT': 4,
            'PAGE': 5,
            'PARAGRAPH': 6
        }
    
    def create_hierarchical_structure(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create enhanced hierarchical structure from extracted documents.
        
        Key improvements over current approach:
        1. Strict hierarchy enforcement (Meeting -> Section -> Item -> Documents)
        2. Enhanced metadata tracking (order, sequence, themes)
        3. Cross-reference detection and linking
        4. Temporal relationship modeling
        5. Thematic grouping across meetings
        """
        # Process documents by type to establish hierarchy
        agendas = [d for d in documents if d.get('document_type') == 'agenda']
        ordinances = [d for d in documents if d.get('document_type') == 'ordinance']
        resolutions = [d for d in documents if d.get('document_type') == 'resolution'] 
        transcripts = [d for d in documents if d.get('document_type') == 'verbatim_transcript']
        
        # Build hierarchy starting from meeting level
        for agenda in agendas:
            self._process_agenda_hierarchy(agenda)
            
        # Link supporting documents to agenda items
        for doc in ordinances + resolutions + transcripts:
            self._link_supporting_document(doc)
            
        # Create enhanced relationships
        self._create_enhanced_relationships()
        
        return {
            'nodes': {node_id: asdict(node) for node_id, node in self.nodes.items()},
            'relationships': self.relationships,
            'hierarchy_metadata': self._generate_hierarchy_metadata()
        }
    
    def _process_agenda_hierarchy(self, agenda: Dict[str, Any]) -> None:
        """Process agenda document to create strict 3-level hierarchy."""
        meeting_date = agenda.get('meeting_info', {}).get('date', 'unknown')
        
        # Level 0: Meeting node (ROOT)
        meeting_id = f"meeting-{meeting_date}"
        meeting_node = HierarchicalNode(
            node_id=meeting_id,
            node_type='MEETING',
            title=f"City Council Meeting - {meeting_date}",
            level=0,
            properties={
                'date': meeting_date,
                'time': agenda.get('meeting_info', {}).get('time'),
                'location': agenda.get('meeting_info', {}).get('location'),
                'document_id': agenda.get('doc_id'),
                'source_file': agenda.get('source_file'),
                'total_agenda_items': len(agenda.get('agenda_items', []))
            }
        )
        self.nodes[meeting_id] = meeting_node
        
        # Group agenda items by section
        sections = self._group_items_by_section(agenda.get('agenda_items', []))
        
        # Level 1: Section nodes
        section_order = 0
        for section_name, items in sections.items():
            section_order += 1
            section_id = f"section-{meeting_date}-{self._normalize_section_name(section_name)}"
            
            section_node = HierarchicalNode(
                node_id=section_id,
                node_type='SECTION',
                title=section_name,
                level=1,
                parent_id=meeting_id,
                properties={
                    'meeting_date': meeting_date,
                    'item_count': len(items),
                    'section_code': self._extract_section_code(section_name),
                    'section_type': self._classify_section_type(section_name),
                    'estimated_duration': sum(self._estimate_item_duration(item) for item in items)
                }
            )
            self.nodes[section_id] = section_node
            meeting_node.children.append(section_id)
            
            # HAS_SECTION relationship (Meeting -> Section)
            self.relationships.append({
                'source': meeting_id,
                'target': section_id,
                'relationship': 'HAS_SECTION',
                'kind': 'STRUCTURAL',
                'order': section_order,
                'section_type': self._classify_section_type(section_name)
            })
            
            # Level 2: Agenda Item nodes
            for item_order, item in enumerate(items, 1):
                item_code = item.get('item_code', f'unknown-{item_order}')
                item_id = f"item-{meeting_date}-{item_code}"
                
                item_node = HierarchicalNode(
                    node_id=item_id,
                    node_type='AGENDA_ITEM',
                    title=item.get('title', 'Unknown Item'),
                    level=2,
                    parent_id=section_id,
                    properties={
                        'item_code': item_code,
                        'document_reference': item.get('document_reference'),
                        'section_name': section_name,
                        'meeting_date': meeting_date,
                        'urls': item.get('urls', []),
                        'has_hyperlinks': bool(item.get('urls', [])),
                        'estimated_duration': self._estimate_item_duration(item),
                        'theme': self._extract_theme(item.get('title', ''))
                    }
                )
                self.nodes[item_id] = item_node
                section_node.children.append(item_id)
                
                # CONTAINS_ITEM relationship (Section -> AgendaItem)
                self.relationships.append({
                    'source': section_id,
                    'target': item_id,
                    'relationship': 'CONTAINS_ITEM',
                    'kind': 'STRUCTURAL',
                    'order': item_order,
                    'item_code': item_code,
                    'has_document_reference': bool(item.get('document_reference'))
                })
                
                # FOLLOWS relationship (AgendaItem -> AgendaItem within section)
                if item_order > 1:
                    prev_item_code = items[item_order - 2].get('item_code', f'unknown-{item_order-1}')
                    prev_item_id = f"item-{meeting_date}-{prev_item_code}"
                    
                    self.relationships.append({
                        'source': prev_item_id,
                        'target': item_id,
                        'relationship': 'FOLLOWS',
                        'kind': 'STRUCTURAL',
                        'sequence': item_order,
                        'section': section_name,
                        'gap_type': self._analyze_sequence_gap(items[item_order - 2], item)
                    })
    
    def _link_supporting_document(self, document: Dict[str, Any]) -> None:
        """Link ordinances, resolutions, and transcripts to agenda items."""
        doc_type = document.get('document_type')
        meeting_date = document.get('meeting_date', 'unknown')
        
        if doc_type == 'verbatim_transcript':
            # Handle transcript linking
            item_codes = document.get('item_codes', [])
            for item_code in item_codes:
                item_id = f"item-{meeting_date}-{item_code}"
                if item_id in self.nodes:
                    transcript_id = f"transcript-{meeting_date}-{item_code}"
                    transcript_node = HierarchicalNode(
                        node_id=transcript_id,
                        node_type='TRANSCRIPT',
                        title=f"Verbatim Transcript - {item_code}",
                        level=3,
                        parent_id=item_id,
                        properties={
                            'item_codes': item_codes,
                            'transcript_type': document.get('transcript_type'),
                            'page_count': len(document.get('pages', [])),
                            'word_count': self._count_words(document.get('full_text', '')),
                            'filename': document.get('metadata', {}).get('filename')
                        }
                    )
                    self.nodes[transcript_id] = transcript_node
                    self.nodes[item_id].children.append(transcript_id)
                    
                    # Add relationship
                    self.relationships.append({
                        'source': item_id,
                        'target': transcript_id,
                        'relationship': 'HAS_TRANSCRIPT',
                        'kind': 'DOCUMENT',
                        'transcript_type': document.get('transcript_type'),
                        'discussion_length': self._estimate_discussion_length(document)
                    })
        
        elif doc_type in ['ordinance', 'resolution']:
            # Handle ordinance/resolution linking
            item_code = document.get('item_code')
            if item_code:
                item_id = f"item-{meeting_date}-{item_code}"
                if item_id in self.nodes:
                    doc_id = f"{doc_type}-{document.get('document_number', 'unknown')}"
                    doc_node = HierarchicalNode(
                        node_id=doc_id,
                        node_type=doc_type.upper(),
                        title=document.get('title', f'{doc_type.title()} {document.get("document_number")}'),
                        level=3,
                        parent_id=item_id,
                        properties={
                            'document_number': document.get('document_number'),
                            'item_code': item_code,
                            'vote_details': document.get('parsed_data', {}).get('vote_details'),
                            'motion_details': document.get('parsed_data', {}).get('motion'),
                            'legal_status': self._determine_legal_status(document),
                            'effective_date': self._extract_effective_date(document)
                        }
                    )
                    self.nodes[doc_id] = doc_node
                    self.nodes[item_id].children.append(doc_id)
                    
                    # Add relationship  
                    self.relationships.append({
                        'source': item_id,
                        'target': doc_id,
                        'relationship': 'REFERENCES_DOCUMENT',
                        'kind': 'DOCUMENT',
                        'document_type': doc_type,
                        'document_number': document.get('document_number'),
                        'legal_binding': doc_type == 'ordinance'
                    })
    
    def _create_enhanced_relationships(self) -> None:
        """Create additional enhanced relationships."""
        # Add cross-references between related agenda items
        self._add_cross_references()
        
        # Add temporal relationships between meetings
        self._add_temporal_relationships()
        
        # Add thematic groupings
        self._add_thematic_relationships()
    
    def _group_items_by_section(self, agenda_items: List[Dict]) -> Dict[str, List[Dict]]:
        """Group agenda items by section name."""
        sections = {}
        for item in agenda_items:
            section_name = item.get('section_name', 'Unknown Section')
            if section_name not in sections:
                sections[section_name] = []
            sections[section_name].append(item)
        return sections
    
    def _normalize_section_name(self, section_name: str) -> str:
        """Normalize section name for ID generation."""
        return re.sub(r'[^a-zA-Z0-9]', '_', section_name.lower())
    
    def _extract_section_code(self, section_name: str) -> Optional[str]:
        """Extract section code from section name."""
        # Look for patterns like "A.", "E.", "F." etc.
        match = re.search(r'\b([A-Z])\b', section_name)
        return match.group(1) if match else None
    
    def _classify_section_type(self, section_name: str) -> str:
        """Classify the type of section."""
        name_lower = section_name.lower()
        if 'presentation' in name_lower or 'protocol' in name_lower:
            return 'ceremonial'
        elif 'ordinance' in name_lower or 'resolution' in name_lower:
            return 'legislative'
        elif 'consent' in name_lower:
            return 'administrative'
        elif 'public' in name_lower and 'comment' in name_lower:
            return 'public_input'
        elif 'manager' in name_lower:
            return 'executive'
        else:
            return 'general'
    
    def _estimate_item_duration(self, item: Dict) -> Optional[int]:
        """Estimate agenda item duration in minutes based on content."""
        title = item.get('title', '')
        if 'presentation' in title.lower():
            return 15
        elif 'ordinance' in title.lower() or 'resolution' in title.lower():
            return 10
        elif 'consent' in title.lower():
            return 2
        else:
            return 5
    
    def _analyze_sequence_gap(self, prev_item: Dict, current_item: Dict) -> str:
        """Analyze the gap between sequential agenda items."""
        prev_code = prev_item.get('item_code', '')
        curr_code = current_item.get('item_code', '')
        
        if prev_code and curr_code:
            prev_letter = prev_code[0] if prev_code else ''
            curr_letter = curr_code[0] if curr_code else ''
            
            if prev_letter != curr_letter:
                return 'section_boundary'
            else:
                return 'within_section'
        return 'unknown'
    
    def _count_words(self, text: str) -> int:
        """Count words in text."""
        return len(text.split()) if text else 0
    
    def _estimate_discussion_length(self, transcript: Dict) -> str:
        """Estimate discussion length category."""
        word_count = self._count_words(transcript.get('full_text', ''))
        if word_count < 500:
            return 'brief'
        elif word_count < 2000:
            return 'moderate' 
        else:
            return 'extensive'
    
    def _determine_legal_status(self, document: Dict) -> str:
        """Determine legal status of ordinance/resolution."""
        vote_details = document.get('parsed_data', {}).get('vote_details', {})
        if vote_details.get('nays', '0') == '0':
            return 'passed_unanimously'
        elif int(vote_details.get('ayes', '0')) > int(vote_details.get('nays', '0')):
            return 'passed_majority'
        else:
            return 'failed'
    
    def _extract_effective_date(self, document: Dict) -> Optional[str]:
        """Extract effective date from document if available."""
        # Look for effective date patterns in document text
        text = document.get('full_text', '')
        effective_pattern = r'effective\s+(?:date|on|as\s+of)\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})'
        match = re.search(effective_pattern, text, re.IGNORECASE)
        return match.group(1) if match else None
    
    def _add_cross_references(self) -> None:
        """Add cross-reference relationships between related agenda items."""
        # Look for items that reference other items in their titles
        for node_id, node in self.nodes.items():
            if node.node_type == 'AGENDA_ITEM':
                title = node.title.lower()
                # Look for references to other items
                item_refs = re.findall(r'\b([A-Z]-\d+)\b', node.title)
                for ref in item_refs:
                    ref_node_id = f"item-{node.properties.get('meeting_date')}-{ref}"
                    if ref_node_id in self.nodes and ref_node_id != node_id:
                        self.relationships.append({
                            'source': node_id,
                            'target': ref_node_id,
                            'relationship': 'CROSS_REFERENCES',
                            'kind': 'CROSS_REF',
                            'reference_type': 'agenda_item'
                        })
    
    def _add_temporal_relationships(self) -> None:
        """Add temporal relationships between meetings."""
        meetings = [node for node in self.nodes.values() if node.node_type == 'MEETING']
        meetings.sort(key=lambda x: x.properties.get('date', ''))
        
        for i in range(1, len(meetings)):
            self.relationships.append({
                'source': meetings[i-1].node_id,
                'target': meetings[i].node_id,
                'relationship': 'PRECEDES',
                'kind': 'TEMPORAL',
                'temporal_type': 'chronological',
                'date_gap': self._calculate_date_gap(
                    meetings[i-1].properties.get('date'),
                    meetings[i].properties.get('date')
                )
            })
    
    def _add_thematic_relationships(self) -> None:
        """Add thematic relationships between related items."""
        # Group items by themes (e.g., budget, zoning, etc.)
        themes = {}
        for node_id, node in self.nodes.items():
            if node.node_type == 'AGENDA_ITEM':
                theme = self._extract_theme(node.title)
                if theme:
                    if theme not in themes:
                        themes[theme] = []
                    themes[theme].append(node_id)
        
        # Add relationships within themes
        for theme, item_ids in themes.items():
            if len(item_ids) > 1:
                for i in range(len(item_ids)):
                    for j in range(i + 1, len(item_ids)):
                        self.relationships.append({
                            'source': item_ids[i],
                            'target': item_ids[j],
                            'relationship': 'THEMATICALLY_RELATED',
                            'kind': 'THEMATIC',
                            'theme': theme
                        })
    
    def _extract_theme(self, title: str) -> Optional[str]:
        """Extract thematic category from agenda item title."""
        title_lower = title.lower()
        themes = {
            'budget': ['budget', 'financial', 'appropriation', 'expenditure'],
            'zoning': ['zoning', 'variance', 'rezoning', 'land_use'],
            'development': ['development', 'construction', 'building', 'permit'],
            'transportation': ['traffic', 'transportation', 'parking', 'road'],
            'public_safety': ['police', 'fire', 'safety', 'emergency'],
            'environment': ['environmental', 'sustainability', 'green', 'climate']
        }
        
        for theme, keywords in themes.items():
            if any(keyword in title_lower for keyword in keywords):
                return theme
        return None
    
    def _calculate_date_gap(self, date1: str, date2: str) -> Optional[int]:
        """Calculate gap in days between two dates."""
        try:
            d1 = datetime.strptime(date1, '%m.%d.%Y')
            d2 = datetime.strptime(date2, '%m.%d.%Y')
            return abs((d2 - d1).days)
        except:
            return None
    
    def _generate_hierarchy_metadata(self) -> Dict[str, Any]:
        """Generate metadata about the hierarchy."""
        node_counts = {}
        for node in self.nodes.values():
            node_counts[node.node_type] = node_counts.get(node.node_type, 0) + 1
        
        relationship_counts = {}
        for rel in self.relationships:
            rel_type = rel.get('relationship', 'UNKNOWN')
            relationship_counts[rel_type] = relationship_counts.get(rel_type, 0) + 1
        
        return {
            'total_nodes': len(self.nodes),
            'total_relationships': len(self.relationships),
            'node_type_distribution': node_counts,
            'relationship_type_distribution': relationship_counts,
            'hierarchy_depth': max(node.level for node in self.nodes.values()) + 1,
            'extraction_timestamp': datetime.now().isoformat()
        }

def main():
    """Example usage of the enhanced hierarchical extractor."""
    print("Enhanced Hierarchical Extractor ready!")
    print("\n🔥 Key Improvements over current approach:")
    print("✅ Strict 3-level hierarchy (Meeting -> Section -> Item)")
    print("✅ Enhanced metadata tracking (order, themes, duration)")
    print("✅ Cross-reference detection between agenda items")
    print("✅ Temporal relationships between meetings")
    print("✅ Thematic grouping and analysis")
    print("✅ Rich relationship properties")

if __name__ == "__main__":
    main() 