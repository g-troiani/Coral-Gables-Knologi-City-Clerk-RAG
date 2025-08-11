# File: scripts/graph_rag_stages/common/test_relationship_standards.py

"""Test and validate relationship standards."""

from relationship_standards import RelationshipStandards

def test_relationship_mappings():
    """Test relationship normalization."""
    
    test_cases = [
        # (input, expected_output)
        ('HAS_AGENDA', 'hasAgenda'),
        ('HAS_SECTION', 'hasSection'),
        ('HAS_AGENDA_ITEM', 'hasAgendaItem'),
        ('IMPLEMENTS', 'implementedBy'),
        ('VOTED_ON', 'votedOn'),
        ('PRECEDES', 'precedes'),
        ('MOVED_BY', 'sponsors'),
        ('SECONDED_BY', 'sponsors'),
        ('EXTRACTED_FROM', None),  # Internal only
        ('hasAgenda', 'hasAgenda'),  # Already canonical
        ('unknown_rel', 'unknown_rel'),  # Unmapped
    ]
    
    print("Testing Relationship Normalization:")
    print("-" * 50)
    
    for input_rel, expected in test_cases:
        result = RelationshipStandards.normalize_relationship(input_rel)
        status = "✅" if result == expected else "❌"
        print(f"{status} {input_rel:20} → {result:20} (expected: {expected})")
    
    print("\n" + "=" * 50)
    print("Relationship Mapping Report:")
    print("=" * 50)
    
    report = RelationshipStandards.get_unmapped_report()
    
    print(f"\nTotal mappings defined: {report['total_mappings']}")
    print(f"Canonical relationships: {report['canonical_relationships']}")
    print(f"Internal-only (filtered): {len(report['internal_only'])}")
    
    print("\nMappings by canonical name:")
    for canonical, variations in sorted(report['mapping_details'].items()):
        print(f"\n  {canonical}:")
        for var in variations:
            print(f"    ← {var}")
    
    if report['unmapped']:
        print("\n⚠️ Unmapped relationships (will pass through as-is):")
        for unmapped in report['unmapped']:
            print(f"  - {unmapped}")

if __name__ == "__main__":
    test_relationship_mappings()
