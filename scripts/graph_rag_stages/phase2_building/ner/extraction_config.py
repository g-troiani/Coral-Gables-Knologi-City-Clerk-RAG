"""
Document-type specific extraction configurations based on real City of Coral Gables documents
"""

EXTRACTION_CONFIGS = {
    'ordinance': {
        'focus_entities': ['Person', 'Organization', 'Location', 'Asset', 'Policy', 'Document', 'LegalReference'],
        'key_relationships': ['sponsors', 'amends', 'governedBy', 'affects', 'authorizedBy', 'votedOn', 'references'],
        'required_attributes': {
            'Policy': ['effectiveDate', 'status', 'legalReferences', 'ordinanceNumber'],
            'Asset': ['value', 'fiscalYear', 'taxingDistrict'],
            'Person': ['title', 'role', 'votePosition'],
            'Organization': ['type', 'jurisdiction', 'relationshipToCity']
        },
        'extraction_hints': [
            "Extract WHEREAS clauses - they contain key organizations (Miami-Dade County, City of Coral Gables), dates, and legal references",
            "Look for County Ordinance references (e.g., 'County Ordinance No. 95-214')",
            "BE IT ORDAINED sections contain the actual policy changes and amendments",
            "Extract vote information: 'Moved: [Person] / Seconded: [Person]', 'Yeas: [list]', 'Nays: [list]'",
            "Signature blocks show Mayor, City Clerk, City Attorney with their formal titles",
            "Special Taxing Districts are specific Organization entities with boundaries and purposes",
            "DocuSign Envelope IDs and legal sufficiency statements are important metadata",
            "Section numbers and repealer provisions show legal structure"
        ],
        'entity_patterns': {
            'Person': ['Mayor [Name]', 'Commissioner [Name]', 'City Attorney [Name]', 'City Clerk [Name]'],
            'Organization': ['City of Coral Gables', 'Miami-Dade County', '[Name] Security Guard District'],
            'Document': ['Ordinance No. [Number]', 'County Ordinance No. [Number]', 'Resolution No. [Number]'],
            'LegalReference': ['Section [Number]', 'Chapter [Number]', 'Code of Metropolitan Dade County']
        }
    },
    'resolution': {
        'focus_entities': ['Person', 'Organization', 'Board', 'Asset', 'Appointment', 'Contract'],
        'key_relationships': ['appoints', 'nominatedBy', 'servesOn', 'awards', 'awardedTo', 'authorizes', 'confirms'],
        'required_attributes': {
            'Person': ['title', 'nominatedBy', 'termLength', 'boardAssignment'],
            'Board': ['name', 'purpose', 'termStructure'],
            'Appointment': ['termStart', 'termEnd', 'boardName', 'appointeeStatus'],
            'Contract': ['vendor', 'amount', 'purpose', 'duration']
        },
        'extraction_hints': [
            "Appointments show 'appointing [Person] (Nominated by [Commissioner])' patterns",
            "Board names include 'Senior Citizens Advisory Board', 'Transportation Advisory Board', etc.",
            "Term information: 'for a two (2) year term which began on [date] and continues through [date]'",
            "Look for 'Nominated by Commissioner [Name]' or 'Nominated by Board-As-A-Whole'",
            "Resolution numbers follow pattern 'Resolution No. 2024-[Number]'",
            "Contract awards mention vendor names, dollar amounts, and project descriptions",
            "Confirm vs. appoint distinction - confirmations are for board-nominated candidates"
        ],
        'entity_patterns': {
            'Person': ['[Name] (Nominated by Commissioner [Name])', '[Name] (Nominated by Board-As-A-Whole)'],
            'Board': ['[Name] Advisory Board', '[Name] Committee', '[Name] Commission'],
            'Organization': ['City of Coral Gables', 'Board-As-A-Whole']
        }
    },
    'verbatim_transcript': {
        'focus_entities': ['Person', 'Action', 'VoteOutcome', 'Presentation', 'PublicComment', 'AgendaItem'],
        'key_relationships': ['performsAction', 'discusses', 'votedOn', 'presents', 'secondsMotion', 'makes motion', 'respondsTo'],
        'required_attributes': {
            'Person': ['title', 'role', 'speakerType'],  # City staff vs. Public speaker vs. Commissioner
            'Action': ['actionType', 'target', 'timestamp'],
            'VoteOutcome': ['voteType', 'result', 'voters'],
            'Presentation': ['presenter', 'topic', 'agendaItem'],
            'PublicComment': ['speaker', 'topic', 'duration']
        },
        'extraction_hints': [
            "City Commission members: 'Mayor [Name]', 'Vice Mayor [Name]', 'Commissioner [Name]'",
            "City Staff: 'City Attorney [Name]', 'City Manager [Name]', 'City Clerk [Name]', 'Planning Official [Name]'",
            "Public speakers are introduced by City Clerk: 'City Clerk [Name]: First speaker today is [Name]'",
            "Motion language: '[Person]: I move to [action]', '[Person] moved', '[Person] seconded'",
            "Timestamps appear as '[Start: XX:XX a.m./p.m.]' or embedded in text",
            "Agenda items referenced as 'Item E-5', 'Agenda Item C', etc.",
            "Presentations include PowerPoint references and Q&A between officials",
            "Public comments often address specific issues (streetlights, zoning, etc.)",
            "Vote calls: 'Yeas:', 'Nays:', 'Unanimous: X-X Vote'",
            "Address references: '405 Biltmore Way, Coral Gables, FL' (City Commission Chambers)"
        ],
        'entity_patterns': {
            'Person': ['Mayor [Name]', 'Vice Mayor [Name]', 'Commissioner [Name]', 'City Attorney [Name]', 
                      'City Manager [Name]', 'City Clerk [Name]', 'Planning Official [Name]', 'Mr./Ms. [Name]'],
            'Location': ['City Commission Chambers', '405 Biltmore Way', 'Coral Gables, FL'],
            'Action': ['moved to [action]', 'seconded [motion]', 'presented [item]', 'discussed [topic]']
        },
        'speaker_roles': {
            'city_commission': ['Mayor', 'Vice Mayor', 'Commissioner'],
            'city_staff': ['City Attorney', 'City Manager', 'City Clerk', 'Planning Official', 'Communications Director'],
            'public': ['Mr.', 'Ms.', 'Dr.', 'Public Speaker']
        }
    },
    'agenda': {
        'focus_entities': ['AgendaItem', 'Section', 'Person', 'Organization', 'Meeting', 'Location'],
        'key_relationships': ['contains', 'schedules', 'assigns', 'recognizes', 'addresses'],
        'required_attributes': {
            'AgendaItem': ['itemCode', 'title', 'section', 'type'],
            'Section': ['sectionCode', 'title', 'purpose'],
            'Meeting': ['date', 'time', 'location', 'type'],
            'Person': ['role', 'recognition_type']  # For awards and recognitions
        },
        'extraction_hints': [
            "Agenda sections: 'PRESENTATIONS AND PROTOCOL DOCUMENTS', 'CONSENT AGENDA', 'PUBLIC COMMENTS'",
            "Item codes follow patterns: 'A.-1', 'B.-1', 'C', 'D.-1', 'E-5', etc.",
            "Meeting info: date, time '9:00 AM', location 'City Commission Chambers'",
            "Commission members listed with full titles",
            "Presentation items include proclamations, recognitions, awards",
            "Board appointments and confirmations in Consent Agenda",
            "Firefighter/Officer of the Month awards with specific recipients",
            "Board meeting minutes presentations requiring no action",
            "Zoom meeting details and public participation instructions"
        ],
        'entity_patterns': {
            'AgendaItem': ['[Code] [Title/Description]'],
            'Section': ['A. PRESENTATIONS AND PROTOCOL DOCUMENTS', 'B. APPROVAL OF MINUTES', 
                       'C. PUBLIC COMMENTS', 'D. CONSENT AGENDA'],
            'Person': ['Mayor [Name]', 'Vice Mayor [Name]', 'Commissioner [Name]', 'Firefighter [Name]', 'Officer [Name]']
        }
    },
    'public_comment': {
        'focus_entities': ['Person', 'Topic', 'Issue', 'Recommendation', 'HistoricalReference'],
        'key_relationships': ['addresses', 'concerns', 'recommends', 'references', 'supports', 'opposes'],
        'required_attributes': {
            'Person': ['speakerName', 'speakerType', 'affiliation'],
            'Topic': ['subject', 'importance', 'cityRelevance'],
            'Issue': ['description', 'impact', 'proposedSolution'],
            'HistoricalReference': ['timeframe', 'relevance', 'context']
        },
        'extraction_hints': [
            "Speakers introduced formally: 'City Clerk [Name]: First speaker today is [Name]'",
            "Topics often relate to city infrastructure (streetlights, zoning, development)",
            "Historical references to city development (1920s, George Merrick era)",
            "Specific locations mentioned (Douglas Entrance, Miracle Mile, Country Club)",
            "Policy discussions often reference specific ordinances or resolutions",
            "Citizens may reference previous meetings or ongoing issues",
            "Presentation materials referenced (PowerPoint, photographs, documents)",
            "Specific recommendations or requests to City Commission",
            "Time limits and procedures mentioned (3-minute rule, etc.)"
        ],
        'entity_patterns': {
            'Person': ['Mr. [Name]', 'Ms. [Name]', 'Dr. [Name]'],
            'Topic': ['streetlights', 'zoning', 'historic preservation', 'development'],
            'Location': ['Douglas Entrance', 'Miracle Mile', 'Country Club', 'DeSoto Fountain']
        }
    }
} 