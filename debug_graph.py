#!/usr/bin/env python3
"""Debug script to inspect what's actually in the Cosmos DB graph."""
import os
import asyncio
import logging
from dotenv import load_dotenv
from gremlin_python.driver import client, serializer
from gremlin_python.process.traversal import T
import json

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE = os.getenv("COSMOS_DATABASE", "cgGraph")
CONTAINER = os.getenv("COSMOS_CONTAINER", "cityClerk")


class GraphDebugger:
    def __init__(self):
        self.client = None
        
    def connect(self):
        """Connect to Cosmos DB."""
        self.client = client.Client(
            f"{COSMOS_ENDPOINT}/gremlin",
            "g",
            username=f"/dbs/{DATABASE}/colls/{CONTAINER}",
            password=COSMOS_KEY,
            message_serializer=serializer.GraphSONSerializersV2d0()
        )
        log.info("Connected to Cosmos DB")
    
    def close(self):
        """Close connection."""
        if self.client:
            self.client.close()
    
    def execute_query(self, query: str):
        """Execute a query and return results."""
        try:
            # Submit the query and get the result set
            result_set = self.client.submit(query)
            # Call .all() to get all results and .result() to resolve the Future
            results = result_set.all().result()
            return results
        except Exception as e:
            log.error(f"Query failed: {e}")
            log.error(f"Query was: {query}")
            return None
    
    def debug_graph(self):
        """Run comprehensive debugging queries."""
        print("\n" + "="*60)
        print("COSMOS DB GRAPH DEBUGGING")
        print("="*60)
        
        # 1. Count all vertices
        print("\n1. VERTEX COUNTS:")
        print("-" * 30)
        total_vertices = self.execute_query("g.V().count()")
        print(f"Total vertices: {total_vertices[0] if total_vertices else 0}")
        
        # 2. Count by label
        print("\n2. VERTICES BY LABEL:")
        print("-" * 30)
        labels_query = "g.V().label().groupCount()"
        labels = self.execute_query(labels_query)
        if labels:
            for label, count in labels[0].items():
                print(f"  {label}: {count}")
        else:
            print("  No vertices found!")
        
        # 3. List all vertices with properties
        print("\n3. ALL VERTICES (first 20):")
        print("-" * 30)
        vertices = self.execute_query("g.V().limit(20).valueMap(true)")
        if vertices:
            for i, vertex in enumerate(vertices):
                print(f"\nVertex {i+1}:")
                vertex_id = str(vertex.get(T.id))
                vertex_label = vertex.get(T.label)
                print(f"  ID: {vertex_id}")
                print(f"  Label: {vertex_label}")
                
                # Extract properties properly
                for key, value in vertex.items():
                    if key not in [T.id, T.label]:
                        # Handle single values vs lists
                        if isinstance(value, list) and len(value) == 1:
                            print(f"  {key}: {value[0]}")
                        else:
                            print(f"  {key}: {value}")
        else:
            print("  No vertices found!")
        
        # 4. Count edges
        print("\n4. EDGE COUNTS:")
        print("-" * 30)
        total_edges = self.execute_query("g.E().count()")
        print(f"Total edges: {total_edges[0] if total_edges else 0}")
        
        # 5. Count edges by label
        print("\n5. EDGES BY LABEL:")
        print("-" * 30)
        edge_labels = self.execute_query("g.E().label().groupCount()")
        if edge_labels and edge_labels[0]:
            for label, count in edge_labels[0].items():
                print(f"  {label}: {count}")
        else:
            print("  No edges found!")
        
        # 6. List sample edges with their connections
        print("\n6. SAMPLE EDGES (first 10):")
        print("-" * 30)
        edges_query = """g.E().limit(10).valueMap(true)"""
        edges = self.execute_query(edges_query)
        if edges:
            for i, edge in enumerate(edges):
                edge_id = str(edge.get(T.id))
                edge_label = edge.get(T.label)
                
                # Get source and target IDs
                out_v = edge.get('OUT')
                in_v = edge.get('IN')
                
                if out_v and in_v:
                    source_id = str(out_v.get(T.id)) if isinstance(out_v, dict) else str(out_v)
                    target_id = str(in_v.get(T.id)) if isinstance(in_v, dict) else str(in_v)
                    print(f"\nEdge {i+1}:")
                    print(f"  {source_id} --[{edge_label}]--> {target_id}")
                    print(f"  Edge ID: {edge_id}")
        else:
            print("  No edges found!")
        
        # 7. Check Meeting nodes specifically
        print("\n7. MEETING NODES:")
        print("-" * 30)
        meetings = self.execute_query("g.V().hasLabel('Meeting').valueMap(true)")
        if meetings:
            for meeting in meetings:
                print(f"\nMeeting:")
                # Extract ID and label properly
                meeting_id = str(meeting.get(T.id))
                meeting_label = meeting.get(T.label)
                print(f"  ID: {meeting_id}")
                print(f"  Label: {meeting_label}")
                
                # Extract properties properly
                for key, value in meeting.items():
                    if key not in [T.id, T.label]:
                        if isinstance(value, list) and len(value) == 1:
                            print(f"  {key}: {value[0]}")
                        else:
                            print(f"  {key}: {value}")
        else:
            print("  No Meeting nodes found!")
        
        # 8. Check AgendaSection nodes
        print("\n8. AGENDA SECTIONS (first 5):")
        print("-" * 30)
        sections = self.execute_query("g.V().hasLabel('AgendaSection').limit(5).valueMap(true)")
        if sections:
            for section in sections:
                print(f"\nSection:")
                # Extract ID and label properly
                section_id = str(section.get(T.id))
                section_label = section.get(T.label)
                print(f"  ID: {section_id}")
                print(f"  Label: {section_label}")
                
                # Extract properties properly
                for key, value in section.items():
                    if key not in [T.id, T.label]:
                        if isinstance(value, list) and len(value) == 1:
                            print(f"  {key}: {value[0]}")
                        else:
                            print(f"  {key}: {value}")
        else:
            print("  No AgendaSection nodes found!")
        
        # 9. Check relationships from meetings
        print("\n9. MEETING RELATIONSHIPS:")
        print("-" * 30)
        meeting_edges_query = """g.V().hasLabel('Meeting').outE().limit(10)
                                .project('from','to','label')
                                .by(outV().id())
                                .by(inV().id())
                                .by(label())"""
        meeting_edges = self.execute_query(meeting_edges_query)
        if meeting_edges:
            for edge in meeting_edges:
                print(f"  {edge['from']} --[{edge['label']}]--> {edge['to']}")
        else:
            print("  No outgoing edges from Meeting nodes!")
        
        # 10. Test a specific meeting structure query
        print("\n10. MEETING STRUCTURE TEST:")
        print("-" * 30)
        structure_test = self.execute_query(
            "g.V().hasLabel('Meeting').as('m').out('HAS_SECTION').as('s').select('m','s').by(id()).limit(5)"
        )
        if structure_test:
            for result in structure_test:
                print(f"  Meeting {result['m']} -> Section {result['s']}")
        else:
            print("  No Meeting->Section relationships found!")
        
        # 11. Check for orphaned nodes
        print("\n11. ORPHANED NODES (no edges):")
        print("-" * 30)
        orphans_query = """g.V().where(__.both().count().is(0)).limit(10)
                          .project('id','label')
                          .by(id())
                          .by(label())"""
        orphans = self.execute_query(orphans_query)
        if orphans:
            for orphan in orphans:
                print(f"  {orphan['label']}: {orphan['id']}")
        else:
            print("  No orphaned nodes found!")
        
        # 12. Database statistics summary
        print("\n12. DATABASE SUMMARY:")
        print("-" * 30)
        vertex_count = self.execute_query("g.V().count()")
        edge_count = self.execute_query("g.E().count()")
        print(f"  Total Vertices: {vertex_count[0] if vertex_count else 0}")
        print(f"  Total Edges: {edge_count[0] if edge_count else 0}")
        
        print("\n" + "="*60)


def main():
    debugger = GraphDebugger()
    debugger.connect()
    
    try:
        debugger.debug_graph()
    finally:
        debugger.close()


if __name__ == "__main__":
    main() 