"""
Unified City Governance Ontology - Single source of truth for all phases
"""

from typing import Dict, List, Set

class UnifiedOntology:
    """Central ontology definition used across all pipeline phases."""
    
    # Phase 1 to Phase 2 entity mappings
    ENTITY_MAPPINGS = {
        'PEOPLE': 'Person',
        'PERSON': 'Person',  # Handle variations
        'ORGANIZATIONS': 'Organization',
        'ORGANIZATION': 'Organization',
        'LOCATIONS': 'Location', 
        'LOCATION': 'Location',
        'PROJECTS': 'Project',
        'PROJECT': 'Project',
        'MONEY': 'Asset',
        'DOCUMENT_NUMBERS': 'Document',
        'DOCUMENT_NUMBER': 'Document',
        'AGENDA_ITEM': 'AgendaItem',
        'SECTION': 'Section'
    }
    
    # Comprehensive entity types (Phase 2)
    ENTITY_TYPES = {
        'Person': {
            'definition': 'An individual involved in or referenced by government activities',
            'attributes': ['personID', 'name', 'title', 'affiliation', 'contactInfo'],
            'examples': ['Mayor Jane Smith', 'Council Member John Doe', 'Commissioner Smith']
        },
        'Organization': {
            'definition': 'A formal group, institution, government body, or department',
            'attributes': ['orgID', 'name', 'type', 'jurisdiction', 'address'],
            'examples': ['City Council', 'Planning Department', 'ABC Corporation']
        },
        'Document': {
            'definition': 'An official record, report, correspondence, or meeting minutes',
            'attributes': ['documentID', 'title', 'type', 'status', 'issueDate', 'version', 'summary', 'sourceURL'],
            'examples': ['Meeting Minutes 01-09-2024', 'Staff Report SR-2024-123']
        },
        'Policy': {
            'definition': 'A formal rule, law, ordinance, resolution, or regulation',
            'attributes': ['policyID', 'title', 'status', 'effectiveDate', 'expirationDate', 'legalReferences'],
            'examples': ['Ordinance 2024-01', 'Resolution R-23-456', 'Emergency Ordinance E-2024-12']
        },
        'Event': {
            'definition': 'A specific planned occurrence like meeting, hearing, or workshop',
            'attributes': ['eventID', 'name', 'type', 'dateTime', 'status', 'outcome'],
            'examples': ['City Commission Regular Meeting', 'Public Hearing on January 23, 2024']
        },
        'Action': {
            'definition': 'A specific procedural step or activity performed',
            'attributes': ['actionID', 'type', 'dateTime', 'outcome', 'details'],
            'examples': ['Vote on Ordinance 2025-12', 'approved', 'deferred', 'amended']
        },
        'Asset': {
            'definition': 'A physical, financial, or other resource of value to the city',
            'attributes': ['assetID', 'name', 'type', 'value', 'currency', 'status', 'fiscalYear'],
            'examples': ['$150,000 Parks Improvement Fund', '$2.5 million Infrastructure Bond']
        },
        'Project': {
            'definition': 'A planned initiative with defined scope, budget, and timeline',
            'attributes': ['projectID', 'name', 'description', 'status', 'startDate', 'endDate'],
            'examples': ['Riverside Greenway Development', 'Main Street Repaving']
        },
        'Location': {
            'definition': 'A physical place, geographical area, or district',
            'attributes': ['locationID', 'name', 'type', 'address', 'coordinates'],
            'examples': ['City Hall', '405 Biltmore Way', 'District 5', 'Miracle Mile']
        },
        'Role': {
            'definition': 'The function or position held by a Person',
            'attributes': ['roleID', 'title', 'startDate', 'endDate'],
            'examples': ['Mayor', 'Committee Chair', 'Sponsor']
        },
        'Topic': {
            'definition': 'Subject matter or issue being discussed',
            'attributes': ['topicID', 'name', 'category', 'description'],
            'examples': ['Affordable Housing', 'Traffic Congestion', 'Zoning']
        },
        'AgendaItem': {
            'definition': 'A specific item on a meeting agenda',
            'attributes': ['itemID', 'title', 'type', 'presenter', 'estimatedDuration'],
            'examples': ['E-1', 'F-10', 'R-2024-123']
        },
        'Section': {
            'definition': 'A grouping of agenda items within a meeting agenda',
            'attributes': ['sectionID', 'name', 'order', 'meetingDate'],
            'examples': ['CONSENT AGENDA', 'ORDINANCES ON FIRST READING', 'CITY MANAGER ITEMS']
        },
        'Contract': {
            'definition': 'A formal agreement between the city and another party',
            'attributes': ['contractID', 'title', 'vendor', 'amount', 'startDate', 'endDate', 'status'],
            'examples': ['Contract No. 2024-15', 'RFP-2023-456']
        },
        'Technology': {
            'definition': 'Software or technical system used by the city',
            'attributes': ['techID', 'name', 'vendor', 'purpose', 'licenseType'],
            'examples': ['Microsoft Teams', 'Granicus', 'Tyler Munis']
        },
        'VoteOutcome': {
            'definition': 'Detailed record of a voting action',
            'attributes': ['outcomeID', 'agendaItemID', 'status', 'yesVotes', 'noVotes', 'abstentions', 'voteDetails'],
            'examples': ['outcome_E-1_2024-01-09', 'Passed 5-2', 'Failed 3-4']
        }
    }
    
    # Relationship types remain the same as in ner_extractor.py
    RELATIONSHIP_TYPES = [
        'isMemberOf', 'isPartOf', 'holdsRole', 'participatesIn', 'authoredBy',
        'sponsors', 'performsAction', 'targetOf', 'recordedIn', 'isLocatedAt',
        'occursAt', 'references', 'amends', 'repeals', 'owns', 'funds',
        'addressesTopic', 'discusses', 'resultsIn', 'governedBy', 'uses',
        'votedOn', 'presents', 'awards', 'awardedTo', 'extractedFrom',
        'hasSection', 'belongsToSection', 'containsItem'
    ]
    
    # Relationship definitions
    RELATIONSHIP_DEFINITIONS = {
        'extractedFrom': {
            'source': ['Person', 'Organization', 'Policy', 'Asset', 'Project', 'Location'],
            'target': 'Document',
            'attributes': ['chunk_id', 'extraction_method', 'source_file'],
            'patterns': ['found in', 'extracted from', 'mentioned in']
        },
        'hasSection': {
            'source': 'Document',
            'target': 'Section', 
            'attributes': ['section_order'],
            'patterns': ['contains section', 'has section', 'divided into']
        },
        'belongsToSection': {
            'source': ['AgendaItem', 'Policy', 'Resolution'],
            'target': 'Section',
            'attributes': ['item_order'],
            'patterns': ['in section', 'part of section', 'listed under']
        },
        'containsItem': {
            'source': 'Section',
            'target': 'AgendaItem',
            'attributes': ['item_order'],
            'patterns': ['contains', 'includes', 'lists']
        }
    }
    
    @classmethod
    def normalize_entity_type(cls, phase1_type: str) -> str:
        """Convert Phase 1 entity type to Phase 2 type."""
        return cls.ENTITY_MAPPINGS.get(phase1_type.upper(), phase1_type)
    
    @classmethod
    def get_entity_categories(cls) -> List[str]:
        """Get all entity type names for directory creation."""
        return list(cls.ENTITY_TYPES.keys())
    
    @classmethod
    def get_id_field_name(cls, entity_type: str) -> str:
        """Get the ID field name for an entity type."""
        return f"{entity_type.lower()}ID"
    
    @classmethod
    def create_entity_prompt_for_phase1(cls) -> str:
        """Create entity extraction prompt for Phase 1 processing."""
        entity_types = ", ".join(cls.ENTITY_MAPPINGS.keys())
        return f"""Extract entities from the following text using the City Governance Ontology.

ENTITY TYPES TO EXTRACT: {entity_types}

Guidelines:
- Extract all mentioned people, organizations, locations, projects, amounts, and document numbers
- For PERSON: Include names, titles, roles in government
- For ORGANIZATION: Include departments, committees, external organizations
- For LOCATION: Include addresses, buildings, districts, geographical areas
- For PROJECT: Include initiatives, programs, development projects
- For MONEY: Include budgets, costs, fees, fines, amounts with dollar values
- For DOCUMENT_NUMBER: Include ordinance numbers, resolution numbers, permit numbers

""" 