# scripts/graph_rag_stages/common/unified_ontology.py

from typing import Dict, List, Union

class UnifiedOntology:
    """Single source of truth for entities & relationships across all stages."""

    # Phase 1 → Phase 2 normalization (add Meeting alias)
    ENTITY_MAPPINGS = {
        'PEOPLE': 'Person', 'PERSON': 'Person',
        'ORGANIZATIONS': 'Organization', 'ORGANIZATION': 'Organization',
        'LOCATIONS': 'Location', 'LOCATION': 'Location',
        'PROJECTS': 'Project', 'PROJECT': 'Project',
        'MONEY': 'Asset',
        'DOCUMENT_NUMBERS': 'Document', 'DOCUMENT_NUMBER': 'Document',
        'AGENDA_ITEM': 'AgendaItem',
        'SECTION': 'Section',
        'MEETING': 'Event'  # Treat Meeting as a specialized Event
    }

    # ==============
    # Entity Types
    # ==============
    ENTITY_TYPES: Dict[str, Dict] = {
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
            'attributes': [
                'documentID','title','type','status','issueDate','version','summary','sourceURL',
                # Model extras:
                'hyperlinks','meetingDate','documentClassification',
                'sourceFileName','sourceFilePath'
            ],
            'examples': ['Meeting Minutes 01-09-2024', 'Staff Report SR-2024-123']
        },
        'Policy': {
            'definition': 'A formal rule, law, ordinance, resolution, or regulation',
            'attributes': [
                'policyID','title','status','effectiveDate','expirationDate','legalReferences',
                # Model extras:
                'ordinanceNumber','documentRepresentation'
            ],
            'examples': ['Ordinance 2024-01', 'Resolution R-23-456', 'Emergency Ordinance E-2024-12']
        },
        'Event': {
            'definition': 'A specific planned occurrence like meeting, hearing, or workshop',
            'attributes': [
                'eventID','name','type','dateTime','status','outcome',
                # Model extras:
                'docId','sourceFileName'
            ],
            'examples': ['City Commission Regular Meeting', 'Public Hearing on January 23, 2024']
        },
        'Action': {
            'definition': 'A specific procedural step or activity performed',
            'attributes': ['actionID','type','dateTime','outcome','details'],
            'examples': ['Vote on Ordinance 2025-12', 'approved', 'deferred', 'amended']
        },
        'Asset': {
            'definition': 'A physical, financial, or other resource of value to the city',
            'attributes': ['assetID','name','type','value','currency','status','fiscalYear'],
            'examples': ['$150,000 Parks Improvement Fund', '$2.5 million Infrastructure Bond']
        },
        'Project': {
            'definition': 'A planned initiative with defined scope, budget, and timeline',
            'attributes': ['projectID','name','description','status','startDate','endDate'],
            'examples': ['Riverside Greenway Development', 'Main Street Repaving']
        },
        'Location': {
            'definition': 'A physical place or district',
            'attributes': ['locationID','name','type','address','coordinates'],
            'examples': ['City Hall', '405 Biltmore Way', 'District 5', 'Miracle Mile']
        },
        'Role': {
            'definition': 'The function or position held by a Person',
            'attributes': ['roleID','title','startDate','endDate'],
            'examples': ['Mayor', 'Committee Chair', 'Sponsor']
        },
        'Topic': {
            'definition': 'Subject matter or issue being discussed',
            'attributes': ['topicID','name','category','description'],
            'examples': ['Affordable Housing', 'Traffic Congestion', 'Zoning']
        },
        'AgendaItem': {
            'definition': 'A specific item on a meeting agenda',
            'attributes': [
                'itemID','title','type','presenter','estimatedDuration',
                # Model extras:
                'meetingDate','documentReference','order','documentType',
                'documentClassification','parentSectionId','sourceURLs','hyperlinks'
            ],
            'examples': ['E-1', 'F-10', 'R-2024-123']
        },
        'Section': {
            'definition': 'A logical grouping within an agenda document',
            'attributes': [
                'sectionID','name','code','sectionType','order','parentAgendaDocId','meetingDate'
            ],
            'examples': ['CONSENT AGENDA', 'PUBLIC COMMENTS', 'REGULAR BUSINESS']
        },
        'AgendaDocument': {
            'definition': 'The formal agenda document for a specific meeting',
            'attributes': [
                'agendaDocID','title','type','status','issueDate','meetingDate',
                'parentMeetingId','sourceFileName','sourceFilePath','sourceURL'
            ],
            'examples': ['Agenda for City Council Meeting 2024-01-09']
        },
        'Contract': {
            'definition': 'A formal agreement between the city and another party',
            'attributes': ['contractID','title','vendor','amount','startDate','endDate','status'],
            'examples': ['Contract No. 2024-15','RFP-2023-456']
        },
        'Technology': {
            'definition': 'Software or technical system used by the city',
            'attributes': ['techID','name','vendor','purpose','licenseType'],
            'examples': ['Microsoft Teams','Granicus','Tyler Munis']
        },
        'VoteOutcome': {
            'definition': 'Detailed record of a voting action',
            'attributes': ['outcomeID','agendaItemID','status','yesVotes','noVotes','abstentions','voteDetails'],
            'examples': ['outcome_E-1_2024-01-09','Passed 5-2','Failed 3-4']
        },
        # Optional: keep Meeting as a thin shell, or rely on ENTITY_MAPPINGS only.
        'Meeting': {
            'definition': 'Alias / specialization of Event representing a meeting',
            'attributes': ['meetingID','date','docId','sourceFileName','sourceURL'],
            'examples': ['City Council Regular Meeting 2024-01-09']
        }
    }

    # Ensure newer types are registered if missing (non-breaking augmentation)
    for _t, _attrs in [
        ('Presentation', {'definition': 'A presentation tied to an agenda item or event', 'attributes': ['presentationID','presenter','topic','agendaItemID']}),
        ('PublicComment', {'definition': 'A public comment during meeting', 'attributes': ['publicCommentID','speaker','topic','duration','position']}),
        ('Board', {'definition': 'A city board or advisory body', 'attributes': ['boardID','name','purpose','termStructure']}),
        ('Appointment', {'definition': 'An appointment to a board or role', 'attributes': ['appointmentID','termStart','termEnd','boardName','appointeeStatus','nominatedBy']}),
        ('LegalReference', {'definition': 'A legal citation or reference', 'attributes': ['legalReferenceID','citation','codeName','jurisdiction','url']}),
    ]:
        if _t not in ENTITY_TYPES:
            ENTITY_TYPES[_t] = _attrs

    # =========================
    # Relationship Definitions
    # =========================
    # NOTE: Use canonical forward names; provide inverseOf for convenience.
    # For legacy names (e.g., belongsTo* or extractedFrom), use aliasOf.
    RELATIONSHIP_DEFINITIONS: Dict[str, Dict[str, Union[str, List[str], Dict]]] = {
        # Membership / hierarchy
        'isMemberOf':   {'source':'Person','target':'Organization','attributes':['startDate','endDate','role']},
        'isPartOf':     {'source':['Organization','AgendaItem'],'target':['Organization','Document','AgendaDocument'],
                         'attributes':['context']},

        # Roles & participation
        'holdsRole':    {'source':'Person','target':'Role','attributes':['startDate','endDate','appointedBy']},
        'participatesIn': {'source':['Person','Organization'],'target':'Event','attributes':['role','capacity']},

        # Authorship / sponsorship
        'authoredBy':   {'source':['Document','Policy','AgendaDocument'],'target':['Person','Organization'],
                         'attributes':['date','role']},
        'sponsors':     {'source':['Person','Organization'],'target':['Policy','Project'],'attributes':['sponsorshipType','date']},

        # Action core
        'performsAction': {'source':['Person','Organization'],'target':'Action','attributes':['timestamp','authority']},
        'targetOf':       {'source':'Action','target':['Document','Policy','Project','Asset'],'attributes':['actionType','outcome']},
        'recordedIn':     {'source':['Action','Event'],'target':['Document','AgendaDocument'],'attributes':['page','section']},

        # Location / timing
        'isLocatedAt': {'source':['Organization','Project'],'target':'Location','attributes':['since','floor','room']},
        'occursAt':    {'source':'Event','target':'Location','attributes':['room','capacity']},

        # References / legal effects
        'references': {'source':['Document','Policy','AgendaDocument'],'target':['Document','Policy','Topic'],'attributes':['context','section']},
        'amends':     {'source':'Policy','target':'Policy','attributes':['amendmentType','sections','effectiveDate']},
        'repeals':    {'source':'Policy','target':'Policy','attributes':['repealDate','reason']},

        # Assets / funding
        'owns':  {'source':['Person','Organization'],'target':'Asset','attributes':['acquisitionDate','ownershipPercentage']},
        'funds': {'source':'Asset','target':['Project','Organization'],'attributes':['amount','fiscalYear','fundingType']},

        # Topics / discussion
        'addressesTopic': {'source':['Document','Event','Project','Section'],'target':'Topic','attributes':['relevance','focus']},
        'discusses':      {'source':'Event','target':['AgendaItem','Policy','Document','Topic'],'attributes':['duration','outcome']},
        'discussedIn':    {'source':['Document','Policy','AgendaItem'],'target':'Event','attributes':['context']},

        # Agenda document structure
        'hasAgenda':     {'source':'Event','target':'AgendaDocument','attributes':['meetingDate']},
        'hasSection':    {'source':'AgendaDocument','target':'Section','attributes':['order']},
        'hasAgendaItem': {'source':'Section','target':'AgendaItem','attributes':['order']},
        'precedes':      {'source':'AgendaItem','target':'AgendaItem','attributes':['orderDifference']},
        'precedesSection': {'source':'Section','target':'Section','attributes':['orderDifference']},
        'resultsIn':     {'source':'AgendaItem','target':'VoteOutcome','attributes':['voteType','unanimous']},

        # Governance of contracts / tech usage
        'governedBy': {'source':'Contract','target':'Policy','attributes':['complianceLevel']},
        'uses':       {'source':'Organization','target':'Technology','attributes':['since','licenseCount','purpose']},

        # Voting & presentations
        'votedOn':  {'source':'VoteOutcome','target':['Policy','Contract','Project'],'attributes':['motionType','conditions']},
        'presents': {'source':'Person','target':'AgendaItem','attributes':['presentationType','duration']},

        # Awards / procurement
        'awards':    {'source':'Organization','target':'Contract','attributes':['awardDate','selectionMethod']},
        'awardedTo': {'source':'Contract','target':'Organization','attributes':['awardAmount','terms']},

        # Policy ↔ Document embodiment & implementation
        'implementedBy': {'source':'Policy','target':'Document','attributes':['documentType','policyType'], 'inverseOf':'implements'},
        'embodies':      {'source':'Document','target':'Policy','attributes':['policyType','legalStatus']},
        'implements':    {'source':'AgendaItem','target':['Policy','Document'],'attributes':['context']},

        # Transcripts & mentions
        'hasTranscript': {'source':['AgendaItem','Event'],'target':'Document','attributes':['kind']},
        'mentionedIn':   {'source':['Person','Organization','Policy','Asset','Project','Location','Topic','Contract','Technology','AgendaItem','VoteOutcome','Role'],
                          'target':'Document','attributes':['chunkId','extractionMethod','sourceFile']},

        # Structural containment (include Document→AgendaItem too)
        'containsItem': {'source':['Section','Document','AgendaDocument'],'target':'AgendaItem','attributes':['order']},

        # ---------- Legacy aliases (keep to avoid breaking existing data) ----------
        'belongsToEvent':  {'aliasOf':'hasAgenda', 'source':'AgendaDocument','target':'Event'},
        'belongsToAgenda': {'aliasOf':'hasSection', 'source':'Section','target':'AgendaDocument'},
        'belongsToSection':{'aliasOf':'hasAgendaItem', 'source':'AgendaItem','target':'Section'},
        'extractedFrom':   {'aliasOf':'mentionedIn', 'source':['Person','Organization','Policy','Asset','Project','Location','Topic'],
                            'target':'Document'}
    }

    # Always derive the public list from the definitions to avoid drift
    RELATIONSHIP_TYPES: List[str] = list(RELATIONSHIP_DEFINITIONS.keys())

    # -------------
    # Helper API
    # -------------
    @classmethod
    def get_entity_categories(cls) -> List[str]:
        return list(cls.ENTITY_TYPES.keys())

    @classmethod
    def normalize_entity_type(cls, phase1_type: str) -> str:
        return cls.ENTITY_MAPPINGS.get((phase1_type or '').upper(), phase1_type)

    @classmethod
    def get_id_field_name(cls, entity_type: str) -> str:
        # canonical: camelCase ID (e.g., personID). Accept snake_case in normalizers.
        et = (entity_type or '').strip()
        if not et: return 'id'
        if et == 'AgendaItem': return 'agendaItemID'
        if et == 'AgendaDocument': return 'agendaDocID'
        return f"{et[0].lower() + et[1:]}ID"

    @classmethod
    def relationship_schema(cls, rel_type: str) -> Dict:
        # Resolve aliases to canonical schema
        schema = cls.RELATIONSHIP_DEFINITIONS.get(rel_type)
        if not schema:
            return {}
        if 'aliasOf' in schema:
            return cls.RELATIONSHIP_DEFINITIONS.get(schema['aliasOf'], {})
        return schema

    @classmethod
    def canonical_rel_type(cls, rel_type: str) -> str:
        schema = cls.RELATIONSHIP_DEFINITIONS.get(rel_type, {})
        return schema.get('aliasOf', rel_type)

    @classmethod
    def inverse_for(cls, rel_type: str) -> str:
        # find inverseOf pointer or reverse scan
        schema = cls.relationship_schema(rel_type)
        inv = schema.get('inverseOf')
        if inv:
            return inv
        # fallback: find any entry pointing at me
        for k, v in cls.RELATIONSHIP_DEFINITIONS.items():
            if isinstance(v, dict) and v.get('inverseOf') == rel_type:
                return k
        return ''

    @classmethod
    def normalize_type_name(cls, type_name: str) -> str:
        """Normalize type name to singular canonical form."""
        if not type_name:
            return type_name
        
        # Handle common plural forms
        plurals_to_singular = {
            'Persons': 'Person',
            'People': 'Person',
            'Organizations': 'Organization',
            'Documents': 'Document',
            'Policies': 'Policy',
            'Events': 'Event',
            'Actions': 'Action',
            'Assets': 'Asset',
            'Projects': 'Project',
            'Locations': 'Location',
            'Roles': 'Role',
            'Topics': 'Topic',
            'AgendaItems': 'AgendaItem',
            'Contracts': 'Contract',
            'Technologies': 'Technology',
            'VoteOutcomes': 'VoteOutcome',
            'Sections': 'Section'
        }
        
        return plurals_to_singular.get(type_name, type_name)

    @classmethod
    def canonicalize_buckets(cls, buckets: Dict) -> Dict:
        """Canonicalize bucket names to handle plural forms from LLM."""
        if not isinstance(buckets, dict):
            return buckets
        
        canonical_buckets = {}
        for bucket_name, entities in buckets.items():
            canonical_name = cls.normalize_type_name(bucket_name)
            if canonical_name in canonical_buckets:
                # Merge with existing bucket
                if isinstance(canonical_buckets[canonical_name], list) and isinstance(entities, list):
                    canonical_buckets[canonical_name].extend(entities)
                else:
                    canonical_buckets[canonical_name] = entities
            else:
                canonical_buckets[canonical_name] = entities
        
        return canonical_buckets