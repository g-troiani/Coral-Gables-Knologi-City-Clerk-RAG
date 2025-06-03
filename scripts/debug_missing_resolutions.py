#!/usr/bin/env python3
"""
Debug script to trace why certain resolutions are not being added to the graph.
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional
import json
import re
from datetime import datetime

from graph_stages.enhanced_document_linker import EnhancedDocumentLinker
from graph_stages.cosmos_db_client import CosmosGraphClient
from graph_stages.agenda_graph_builder import AgendaGraphBuilder

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger('debug_resolutions')


class ResolutionDebugger:
    """Debug tool for tracing resolution processing issues."""
    
    def __init__(self):
        self.document_linker = EnhancedDocumentLinker()
        self.cosmos_client = None
        self.graph_builder = None
        self.debug_data = {
            "timestamp": datetime.now().isoformat(),
            "target_resolutions": ["2024-04", "2024-05", "2024-06", "2024-07"],
            "meeting_date": "01.09.2024",
            "findings": {}
        }
    
    async def initialize(self):
        """Initialize connections."""
        self.cosmos_client = CosmosGraphClient()
        await self.cosmos_client.connect()
        self.graph_builder = AgendaGraphBuilder(self.cosmos_client, upsert_mode=True)
        log.info("✅ Debugger initialized")
    
    async def debug_full_pipeline(self, 
                                  meeting_date: str,
                                  resolutions_dir: Path,
                                  agenda_json_dir: Path):
        """Run full debugging trace."""
        log.info(f"🔍 Starting debug for meeting date: {meeting_date}")
        log.info(f"📁 Resolutions directory: {resolutions_dir}")
        log.info(f"📁 Agenda JSON directory: {agenda_json_dir}")
        
        # Step 1: Check file system
        await self._debug_filesystem(meeting_date, resolutions_dir)
        
        # Step 2: Check agenda extraction
        await self._debug_agenda_extraction(meeting_date, agenda_json_dir)
        
        # Step 3: Check document linking
        await self._debug_document_linking(meeting_date, resolutions_dir)
        
        # Step 4: Check graph state
        await self._debug_graph_state(meeting_date)
        
        # Step 5: Generate report
        self._generate_debug_report()
    
    async def _debug_filesystem(self, meeting_date: str, resolutions_dir: Path):
        """Debug step 1: Check if files exist in filesystem."""
        log.info("\n📂 STEP 1: Checking filesystem...")
        
        # Convert date format
        date_underscore = meeting_date.replace(".", "_")
        
        # List all files in resolutions directory
        all_files = []
        if resolutions_dir.exists():
            all_files = list(resolutions_dir.rglob("*.pdf"))
            log.info(f"Total PDF files in resolutions directory: {len(all_files)}")
        else:
            log.error(f"❌ Resolutions directory does not exist: {resolutions_dir}")
            self.debug_data["findings"]["filesystem"] = {
                "error": "Resolutions directory does not exist",
                "path": str(resolutions_dir)
            }
            return
        
        # Check for target resolutions
        target_found = {}
        for target in self.debug_data["target_resolutions"]:
            found_files = []
            for file in all_files:
                if target in file.name and date_underscore in file.name:
                    found_files.append(str(file))
                    log.info(f"✅ Found {target}: {file.name} at {file.parent}")
            
            if not found_files:
                # Try alternative patterns
                alt_patterns = [
                    f"{target}*{date_underscore}*.pdf",
                    f"{target}*{meeting_date.replace('.', '-')}*.pdf",
                    f"{target}*.pdf"
                ]
                for pattern in alt_patterns:
                    matches = list(resolutions_dir.rglob(pattern))
                    if matches:
                        found_files.extend([str(m) for m in matches])
                        log.info(f"✅ Found {target} with pattern {pattern}: {[m.name for m in matches]}")
                        break
            
            target_found[target] = found_files
            if not found_files:
                log.warning(f"❌ NOT FOUND: {target} for date {meeting_date}")
        
        self.debug_data["findings"]["filesystem"] = {
            "total_files": len(all_files),
            "target_resolutions_found": target_found,
            "sample_filenames": [f.name for f in all_files[:10]]
        }
    
    async def _debug_agenda_extraction(self, meeting_date: str, agenda_json_dir: Path):
        """Debug step 2: Check if resolutions appear in extracted agenda."""
        log.info("\n📋 STEP 2: Checking agenda extraction...")
        
        # Find ontology file
        ontology_files = list(agenda_json_dir.glob(f"*{meeting_date.replace('.', '*')}*_ontology.json"))
        
        if not ontology_files:
            log.error(f"❌ No ontology file found for date {meeting_date}")
            self.debug_data["findings"]["agenda_extraction"] = {
                "error": "No ontology file found"
            }
            return
        
        ontology_file = ontology_files[0]
        log.info(f"📄 Using ontology file: {ontology_file.name}")
        
        # Load and analyze ontology
        with open(ontology_file, 'r') as f:
            ontology = json.load(f)
        
        # Find all F-section items (resolutions)
        f_items = []
        all_items = []
        
        for section in ontology.get('sections', []):
            for item in section.get('items', []):
                all_items.append(item)
                if item.get('item_code', '').startswith('F'):
                    f_items.append(item)
                    log.info(f"📌 Found F-item: {item['item_code']} - {item.get('document_reference', 'NO_REF')}")
        
        # Check for our target resolutions
        target_items = {}
        for target in self.debug_data["target_resolutions"]:
            found = False
            for item in all_items:
                if target in str(item.get('document_reference', '')):
                    target_items[target] = item
                    found = True
                    log.info(f"✅ Found {target} in agenda as item {item['item_code']}")
                    break
            
            if not found:
                log.warning(f"❌ {target} NOT in agenda items")
                # Check if it might be in the text but not extracted
                self._search_in_agenda_text(target, ontology_file.parent)
        
        self.debug_data["findings"]["agenda_extraction"] = {
            "ontology_file": ontology_file.name,
            "total_items": len(all_items),
            "f_items_count": len(f_items),
            "f_items": [{"code": item['item_code'], "ref": item.get('document_reference')} for item in f_items],
            "target_items_found": {k: v.get('item_code', 'NOT_FOUND') for k, v in target_items.items()}
        }
    
    def _search_in_agenda_text(self, target: str, agenda_dir: Path):
        """Search for resolution reference in raw agenda text."""
        # Look for extracted text file
        text_files = list(agenda_dir.glob("*_extracted.json"))
        for text_file in text_files:
            try:
                with open(text_file, 'r') as f:
                    data = json.load(f)
                    full_text = data.get('full_text', '')
                    if target in full_text:
                        # Find context
                        index = full_text.find(target)
                        context = full_text[max(0, index-200):index+200]
                        log.info(f"🔍 Found {target} in raw text but not extracted as item:")
                        log.info(f"   Context: ...{context}...")
            except:
                pass
    
    async def _debug_document_linking(self, meeting_date: str, resolutions_dir: Path):
        """Debug step 3: Trace document linking process."""
        log.info("\n🔗 STEP 3: Debugging document linking...")
        
        # Run the document linker with debug logging
        debug_dir = Path("city_clerk_documents/graph_json/debug")
        debug_dir.mkdir(exist_ok=True)
        
        # Mock ordinances directory for the enhanced linker
        dummy_ordinances = Path("dummy")
        
        try:
            linked_docs = await self.document_linker.link_documents_for_meeting(
                meeting_date,
                dummy_ordinances,
                resolutions_dir
            )
            
            resolutions = linked_docs.get("resolutions", [])
            log.info(f"📊 Document linker found {len(resolutions)} resolutions")
            
            # Check our targets
            target_linking = {}
            for target in self.debug_data["target_resolutions"]:
                found = None
                for res in resolutions:
                    if target == res.get('document_number'):
                        found = res
                        log.info(f"✅ {target} linked with item code: {res.get('item_code', 'NONE')}")
                        break
                
                if not found:
                    log.warning(f"❌ {target} NOT linked by document linker")
                
                target_linking[target] = {
                    "found": found is not None,
                    "item_code": found.get('item_code') if found else None,
                    "title": found.get('title', '')[:100] if found else None
                }
            
            self.debug_data["findings"]["document_linking"] = {
                "total_resolutions_linked": len(resolutions),
                "target_linking": target_linking,
                "all_linked": [{"num": r['document_number'], "code": r.get('item_code')} 
                              for r in resolutions]
            }
            
        except Exception as e:
            log.error(f"❌ Document linking failed: {e}")
            import traceback
            traceback.print_exc()
            self.debug_data["findings"]["document_linking"] = {
                "error": str(e)
            }
    
    async def _debug_graph_state(self, meeting_date: str):
        """Debug step 4: Check current graph state."""
        log.info("\n🕸️ STEP 4: Checking graph state...")
        
        # Convert date format for graph IDs
        date_dashes = meeting_date.replace('.', '-')
        
        # Check for resolution nodes
        target_nodes = {}
        for target in self.debug_data["target_resolutions"]:
            node_id = f"resolution-{target}"
            exists = await self.cosmos_client.vertex_exists(node_id)
            
            if exists:
                # Get node details
                node = await self.cosmos_client.get_vertex(node_id)
                log.info(f"✅ Node exists: {node_id}")
                log.info(f"   Properties: {node}")
                
                # Check edges
                edges_query = f"g.V('{node_id}').bothE().count()"
                edge_count = await self.cosmos_client._execute_query(edges_query)
                log.info(f"   Edge count: {edge_count[0] if edge_count else 0}")
                
                target_nodes[target] = {
                    "exists": True,
                    "properties": dict(node) if node else {},
                    "edge_count": edge_count[0] if edge_count else 0
                }
            else:
                log.warning(f"❌ Node NOT FOUND: {node_id}")
                target_nodes[target] = {"exists": False}
        
        # Check for agenda items that should link to these resolutions
        log.info("\n🔍 Checking agenda items in graph...")
        for target in self.debug_data["target_resolutions"]:
            # Try different item code patterns
            possible_codes = [f"F-{target.split('-')[1]}", f"F.-{target.split('-')[1]}."]
            for code in possible_codes:
                item_id = f"item-{date_dashes}-{code.replace('.', '')}"
                exists = await self.cosmos_client.vertex_exists(item_id)
                if exists:
                    log.info(f"✅ Agenda item exists: {item_id}")
                    break
                else:
                    log.info(f"❓ Agenda item not found: {item_id}")
        
        self.debug_data["findings"]["graph_state"] = {
            "target_nodes": target_nodes,
            "date_format_used": date_dashes
        }
    
    def _generate_debug_report(self):
        """Generate comprehensive debug report."""
        report_path = Path("city_clerk_documents/graph_json/debug/resolution_debug_report.json")
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            json.dump(self.debug_data, f, indent=2)
        
        log.info(f"\n📊 Debug report saved to: {report_path}")
        
        # Print summary
        log.info("\n" + "="*60)
        log.info("DEBUGGING SUMMARY")
        log.info("="*60)
        
        for step, findings in self.debug_data["findings"].items():
            log.info(f"\n{step.upper()}:")
            if "error" in findings:
                log.info(f"  ❌ ERROR: {findings['error']}")
            else:
                log.info(f"  ✅ Completed")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.cosmos_client:
            await self.cosmos_client.close()


async def main():
    """Run the debugger."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Debug missing resolutions")
    parser.add_argument("--meeting-date", default="01.09.2024", 
                       help="Meeting date in MM.DD.YYYY format")
    parser.add_argument("--resolutions-dir", type=Path,
                       default=Path("city_clerk_documents/global/City Comissions 2024/Resolutions"),
                       help="Resolutions directory")
    parser.add_argument("--agenda-dir", type=Path,
                       default=Path("city_clerk_documents/graph_json"),
                       help="Directory with extracted agenda JSONs")
    
    args = parser.parse_args()
    
    debugger = ResolutionDebugger()
    await debugger.initialize()
    
    try:
        await debugger.debug_full_pipeline(
            args.meeting_date,
            args.resolutions_dir,
            args.agenda_dir
        )
    finally:
        await debugger.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 