# Split API Extraction Approach

## Overview

This implementation splits the entity extraction into **two focused API calls**, each handling approximately 50% of the entity types and their related relationships. This approach can improve extraction accuracy and manage complexity for documents with diverse content.

## Entity Groups

### Group 1: Governance Entities (~9 types)
Focus on people, organizations, and governance activities:
- **Person** - Individuals involved in government
- **Organization** - Departments, committees, bodies
- **Role** - Positions and functions
- **Event** - Meetings, hearings, workshops
- **Meeting** - Specialized event type
- **Action** - Procedural steps, motions, votes
- **VoteOutcome** - Detailed voting records
- **Appointment** - Board/role appointments
- **Board** - Advisory bodies

### Group 2: Documents & Infrastructure (~14 types)
Focus on documents, policies, and physical/digital infrastructure:
- **Document** - Reports, minutes, correspondence
- **Policy** - Ordinances, resolutions, regulations
- **AgendaItem** - Meeting agenda items
- **Section** - Agenda groupings
- **AgendaDocument** - Complete agenda documents
- **Location** - Physical places, addresses
- **Asset** - Financial/physical resources
- **Project** - City initiatives
- **Topic** - Subject matters
- **Contract** - Formal agreements
- **Technology** - Software systems
- **Presentation** - Meeting presentations
- **PublicComment** - Citizen input
- **LegalReference** - Citations, statutes

## How It Works

### 1. Two Focused Prompts
Each API call receives:
- A focused ontology containing only relevant entity types
- Instructions to extract ONLY those entity types
- The same document text

### 2. Extraction Process
```python
# API Call 1: Extract governance entities
group1_triples = extract_governance_entities(text)
# Extracts: Person, Organization, Action, performsAction, etc.

# API Call 2: Extract documents & infrastructure
group2_triples = extract_document_entities(text)
# Extracts: Document, Policy, Location, authoredBy, etc.

# Merge results
merged_triples = merge_and_deduplicate(group1_triples, group2_triples)
```

### 3. Relationship Handling
- Each group extracts relationships where source OR target is in their entity set
- Cross-group relationships (e.g., Person → Document) can be captured by either call
- Deduplication ensures no duplicate triples in final output

## Usage

### Create Split Prompts
```bash
python simple_ner_split_api.py --create-prompts
```
This creates:
- `ner_prompt_group1.txt` - Governance entities prompt
- `ner_prompt_group2.txt` - Documents & infrastructure prompt
- `ontology_group1_governance.txt` - Focused ontology for group 1
- `ontology_group2_documents.txt` - Focused ontology for group 2

### Process a Chunk
```bash
python simple_ner_split_api.py --chunk-file path/to/chunk.txt
```

### Test the Approach
```bash
python test_split_api_extraction.py
```

## Benefits

### 1. **Improved Focus**
- Each LLM call has a narrower scope
- Reduces cognitive load on the model
- Can improve extraction accuracy

### 2. **Better Error Handling**
- If one API call fails, you still get partial results
- Easier to debug which entity types are problematic

### 3. **Flexible Scaling**
- Can adjust group sizes based on document types
- Easy to add a third group if needed
- Can run groups in parallel

### 4. **Specialized Prompting**
- Can tune prompts differently for each group
- E.g., more examples for complex governance relationships

## Trade-offs

### 1. **Performance**
- Two API calls instead of one (~2x slower)
- Additional merging overhead
- More tokens used overall

### 2. **Cross-Group Relationships**
- May miss some subtle cross-group relationships
- Requires careful group design
- Deduplication complexity

### 3. **Cost**
- Double the API calls = double the cost
- Useful for high-value documents where accuracy matters

## Example Output

```json
{
  "chunk": "test_comprehensive.txt",
  "entities_extracted": 15,
  "relationships_extracted": 12,
  "group1_triples": 8,    // Person, Organization, Action, etc.
  "group2_triples": 10,   // Document, Policy, Location, etc.
  "merged_triples": 17,   // After deduplication
  "entity_log": {...},
  "relationship_log": {...}
}
```

## When to Use

### Use Split API When:
- Documents contain diverse entity types
- Accuracy is more important than speed
- You need detailed extraction metrics
- Debugging complex extraction issues

### Use Single API When:
- Speed is critical
- Documents are focused on one domain
- Cost is a primary concern
- Simple, straightforward content

## Implementation Details

### Deduplication
- Triples are deduplicated by (subject_id, predicate, object_id)
- Entity attributes are merged (non-null values preserved)
- Same ID generation rules apply

### Output Format
- Identical to single API approach
- Same persistence logic
- Full backward compatibility

## Future Enhancements

1. **Dynamic Grouping** - Analyze document type to choose optimal groups
2. **Parallel Execution** - Run both API calls simultaneously
3. **Confidence Scoring** - Weight results based on group relevance
4. **Adaptive Splitting** - Adjust groups based on extraction performance
