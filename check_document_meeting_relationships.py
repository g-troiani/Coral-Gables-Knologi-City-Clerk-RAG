#!/usr/bin/env python3
"""
Script to check if all agenda, transcript, ordinance, and resolution documents 
have a meeting relationship in the Cosmos database.
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict
import os
import re
from dotenv import load_dotenv

from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def check_document_meeting_relationships():
    """Check if all documents have meeting relationships."""
    
    # Initialize Cosmos client with environment variables
    cosmos_client = CosmosGraphClient()
    
    try:
        async with cosmos_client:
            # Get all documents categorized by type
            documents = await get_documents_by_type(cosmos_client)
            
            # Get all meetings
            meetings = await get_all_meetings(cosmos_client)
            
            # Check relationships
            results = await check_relationships(cosmos_client, documents, meetings)
            
            # Report results
            print_results(results, documents, meetings)
            
    except Exception as e:
        log.error(f"Error checking relationships: {e}")
        raise


async def get_documents_by_type(cosmos_client: CosmosGraphClient) -> Dict[str, List[Dict]]:
    """Get all documents categorized by type based on source_file and title."""
    documents = {
        'agenda': [],
        'transcript': [],
        'ordinance': [],
        'resolution': []
    }
    
    # Get all Document vertices
    try:
        query = "g.V().hasLabel('Document').valueMap(true)"
        all_documents = await cosmos_client._execute_query(query)
        
        if not all_documents:
            log.warning("No Document vertices found")
            return documents
        
        # Categorize documents based on source_file and title
        for doc in all_documents:
            source_file = doc.get('source_file', [''])[0] if isinstance(doc.get('source_file'), list) else doc.get('source_file', '')
            title = doc.get('title', [''])[0] if isinstance(doc.get('title'), list) else doc.get('title', '')
            
            # Convert to strings for analysis
            source_file_str = str(source_file).lower()
            title_str = str(title).lower()
            
            # Classify based on filename patterns
            if 'agenda' in source_file_str or 'agenda' in title_str:
                documents['agenda'].append(doc)
            elif 'transcript' in source_file_str or 'transcript' in title_str or 'verbatim' in source_file_str or 'verbatim' in title_str:
                documents['transcript'].append(doc)
            elif 'ordinance' in source_file_str or 'ordinance' in title_str:
                documents['ordinance'].append(doc)
            elif 'resolution' in source_file_str or 'resolution' in title_str:
                documents['resolution'].append(doc)
        
        # Log results
        for doc_type, docs in documents.items():
            log.info(f"Found {len(docs)} {doc_type} documents")
            
    except Exception as e:
        log.error(f"Error querying documents: {e}")
    
    return documents


async def get_all_meetings(cosmos_client: CosmosGraphClient) -> List[Dict]:
    """Get all meeting vertices."""
    try:
        query = "g.V().hasLabel('meeting').valueMap(true)"
        result = await cosmos_client._execute_query(query)
        
        meetings = result or []
        log.info(f"Found {len(meetings)} meetings")
        return meetings
        
    except Exception as e:
        log.error(f"Error querying meetings: {e}")
        return []


async def check_relationships(cosmos_client: CosmosGraphClient, documents: Dict[str, List[Dict]], meetings: List[Dict]) -> Dict:
    """Check relationships between documents and meetings."""
    results = {
        'agenda_to_meetings': {},
        'transcript_to_meetings': {},
        'ordinance_to_meetings': {},
        'resolution_to_meetings': {},
        'unlinked_documents': defaultdict(list)
    }
    
    # Create a mapping of meeting dates to meeting IDs for easier lookup
    meeting_date_to_id = {}
    for meeting in meetings:
        meeting_date = meeting.get('date', [''])[0] if isinstance(meeting.get('date'), list) else meeting.get('date', '')
        meeting_id = meeting.get('id', '')
        if meeting_date and meeting_id:
            meeting_date_to_id[meeting_date] = meeting_id
    
    # Check each document type
    for doc_type in ['agenda', 'transcript', 'ordinance', 'resolution']:
        for doc in documents[doc_type]:
            doc_id = doc.get('id', '')
            if not doc_id:
                continue
                
            # Try to find relationships
            relationships = await find_document_relationships(cosmos_client, doc_id, doc, meeting_date_to_id)
            
            results[f'{doc_type}_to_meetings'][doc_id] = relationships
            
            if not relationships:
                results['unlinked_documents'][doc_type].append(doc)
    
    return results


async def find_document_relationships(cosmos_client: CosmosGraphClient, doc_id: str, doc: Dict, meeting_date_to_id: Dict[str, str]) -> List[Dict]:
    """Find meeting relationships for a document."""
    relationships = []
    
    try:
        # Method 1: Check for direct edges to meetings
        direct_edges_query = f"g.V('{doc_id}').outE().inV().hasLabel('meeting').valueMap(true)"
        direct_results = await cosmos_client._execute_query(direct_edges_query)
        if direct_results:
            relationships.extend(direct_results)
        
        # Method 2: Check for relationship via meeting_date property
        meeting_date = doc.get('meeting_date', [''])[0] if isinstance(doc.get('meeting_date'), list) else doc.get('meeting_date', '')
        if meeting_date and meeting_date in meeting_date_to_id:
            meeting_id = meeting_date_to_id[meeting_date]
            meeting_query = f"g.V('{meeting_id}').valueMap(true)"
            meeting_result = await cosmos_client._execute_query(meeting_query)
            if meeting_result:
                relationships.extend(meeting_result)
        
        # Method 3: Try to extract meeting date from source_file and match
        source_file = doc.get('source_file', [''])[0] if isinstance(doc.get('source_file'), list) else doc.get('source_file', '')
        if source_file and not relationships:
            # Extract date patterns from filename
            date_patterns = [
                r'(\d{2})\.(\d{2})\.(\d{4})',  # MM.DD.YYYY
                r'(\d{2})-(\d{2})-(\d{4})',   # MM-DD-YYYY
                r'(\d{4})-(\d{2})-(\d{2})',   # YYYY-MM-DD
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, source_file)
                if match:
                    # Try different date formats
                    groups = match.groups()
                    if len(groups) == 3:
                        # Try MM-DD-YYYY format first
                        possible_dates = [
                            f"{groups[0]}-{groups[1]}-{groups[2]}",  # MM-DD-YYYY
                            f"{groups[2]}-{groups[0]}-{groups[1]}",  # YYYY-MM-DD
                        ]
                        
                        for date_str in possible_dates:
                            if date_str in meeting_date_to_id:
                                meeting_id = meeting_date_to_id[date_str]
                                meeting_query = f"g.V('{meeting_id}').valueMap(true)"
                                meeting_result = await cosmos_client._execute_query(meeting_query)
                                if meeting_result:
                                    relationships.extend(meeting_result)
                                    break
                    
                    if relationships:
                        break
        
        # Method 4: Check if there are agenda items that link to meetings
        agenda_items_query = f"g.V('{doc_id}').out().hasLabel('agendaItem').valueMap(true)"
        agenda_items = await cosmos_client._execute_query(agenda_items_query)
        
        if agenda_items:
            for item in agenda_items:
                item_meeting_date = item.get('meeting_date', [''])[0] if isinstance(item.get('meeting_date'), list) else item.get('meeting_date', '')
                if item_meeting_date and item_meeting_date in meeting_date_to_id:
                    meeting_id = meeting_date_to_id[item_meeting_date]
                    meeting_query = f"g.V('{meeting_id}').valueMap(true)"
                    meeting_result = await cosmos_client._execute_query(meeting_query)
                    if meeting_result:
                        relationships.extend(meeting_result)
                        break
        
        # Remove duplicates
        seen_ids = set()
        unique_relationships = []
        for rel in relationships:
            rel_id = rel.get('id', '')
            if rel_id and rel_id not in seen_ids:
                seen_ids.add(rel_id)
                unique_relationships.append(rel)
        
        return unique_relationships
        
    except Exception as e:
        log.debug(f"Error finding relationships for {doc_id}: {e}")
        return []


def print_results(results: Dict, documents: Dict[str, List[Dict]], meetings: List[Dict]) -> None:
    """Print the analysis results."""
    print("\n" + "="*60)
    print("DOCUMENT-MEETING RELATIONSHIP ANALYSIS")
    print("="*60)
    
    # Summary statistics
    total_docs = sum(len(docs) for docs in documents.values())
    total_meetings = len(meetings)
    
    print(f"\nSUMMARY:")
    print(f"  Total documents: {total_docs}")
    print(f"  Total meetings: {total_meetings}")
    print(f"  - Agenda documents: {len(documents['agenda'])}")
    print(f"  - Transcript documents: {len(documents['transcript'])}")
    print(f"  - Ordinance documents: {len(documents['ordinance'])}")
    print(f"  - Resolution documents: {len(documents['resolution'])}")
    
    # Check each document type
    print(f"\nRELATIONSHIP STATUS:")
    
    for doc_type in ['agenda', 'transcript', 'ordinance', 'resolution']:
        total_docs_type = len(documents[doc_type])
        unlinked_docs = len(results['unlinked_documents'][doc_type])
        linked_docs = total_docs_type - unlinked_docs
        
        if total_docs_type > 0:
            percentage = (linked_docs / total_docs_type) * 100
            print(f"  {doc_type.capitalize()} documents:")
            print(f"    - Linked to meetings: {linked_docs}/{total_docs_type} ({percentage:.1f}%)")
            print(f"    - Unlinked: {unlinked_docs}")
        else:
            print(f"  {doc_type.capitalize()} documents: 0 found")
    
    # Show some examples of unlinked documents
    print(f"\nUNLINKED DOCUMENTS (examples):")
    for doc_type, unlinked_docs in results['unlinked_documents'].items():
        if unlinked_docs:
            print(f"  {doc_type.capitalize()} documents without meeting relationships:")
            for doc in unlinked_docs[:3]:  # Show first 3
                doc_id = doc.get('id', 'unknown')
                source_file = doc.get('source_file', ['unknown'])[0] if isinstance(doc.get('source_file'), list) else doc.get('source_file', 'unknown')
                meeting_date = doc.get('meeting_date', ['unknown'])[0] if isinstance(doc.get('meeting_date'), list) else doc.get('meeting_date', 'unknown')
                print(f"    - {doc_id}")
                print(f"      Source: {source_file}")
                print(f"      Meeting date: {meeting_date}")
            if len(unlinked_docs) > 3:
                print(f"    ... and {len(unlinked_docs) - 3} more")
    
    # Overall answer
    print(f"\nANSWER TO QUESTION:")
    all_linked = all(len(results['unlinked_documents'][doc_type]) == 0 for doc_type in ['agenda', 'transcript', 'ordinance', 'resolution'] if documents[doc_type])
    
    if all_linked:
        print("  ✅ YES - All agenda, transcript, ordinance, and resolution documents have meeting relationships in Cosmos")
    else:
        print("  ❌ NO - Some documents are missing meeting relationships in Cosmos")
        
        # Show which types have issues
        problematic_types = [doc_type for doc_type in ['agenda', 'transcript', 'ordinance', 'resolution'] 
                           if documents[doc_type] and results['unlinked_documents'][doc_type]]
        
        if problematic_types:
            print(f"  Document types with missing relationships: {', '.join(problematic_types)}")
    
    # Additional insights
    print(f"\nADDITIONAL INSIGHTS:")
    print(f"  - The database uses a hierarchical structure: Meeting -> Section -> AgendaItem")
    print(f"  - Documents are primarily identified by source_file patterns")
    print(f"  - Meeting relationships can be inferred through:")
    print(f"    1. Direct edges to meeting vertices")
    print(f"    2. meeting_date property matches")
    print(f"    3. Date extraction from source_file names")
    print(f"    4. Relationships via agenda items")


if __name__ == "__main__":
    asyncio.run(check_document_meeting_relationships()) 