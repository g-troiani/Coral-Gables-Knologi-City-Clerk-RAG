#!/usr/bin/env python3
"""
Enhanced Entity Deduplication for GraphRAG output.

This module provides sophisticated entity deduplication capabilities including:
- Partial name matching ("Vince Lago" matches "Lago")
- Token-based overlap
- Semantic similarity using TF-IDF
- Graph structure analysis
- Abbreviation matching ("V. Lago" matches "Vince Lago")
- Role-based matching ("Mayor" matches "Mayor Lago")
- Multiple scoring strategies with configurable weights
"""

import pandas as pd
import networkx as nx
from pathlib import Path
import difflib
from typing import Dict, List, Tuple, Set, Any, Optional, Union
import logging
import json
import re
from datetime import datetime
from collections import defaultdict
import math
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp

# Optional dependencies with fallbacks
try:
    import Levenshtein
    HAS_LEVENSHTEIN = True
except ImportError:
    HAS_LEVENSHTEIN = False
    
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

logger = logging.getLogger(__name__)

def _compare_entity_pairs_worker(args):
    """Worker function for parallel entity pair comparison."""
    entity_pairs, config, weights = args
    
    candidates = []
    
    for entity1, entity2 in entity_pairs:
        # Calculate similarity scores
        scores = _calculate_similarity_scores_standalone(entity1, entity2, config)
        
        # Calculate combined score
        combined_score = _calculate_combined_score_standalone(scores, weights)
        
        # Check threshold
        if combined_score >= config.get('min_combined_score', 0.7):
            # Validate candidate
            scores['combined_score'] = combined_score
            if _validate_merge_candidate_standalone(entity1, entity2, scores):
                # Determine merge reason
                merge_reason = _determine_merge_reason_standalone(scores)
                
                candidates.append({
                    'entity1_title': entity1['title'],
                    'entity2_title': entity2['title'],
                    'entity1_id': entity1.get('id', entity1.get('human_readable_id', '')),
                    'entity2_id': entity2.get('id', entity2.get('human_readable_id', '')),
                    'combined_score': combined_score,
                    'individual_scores': scores,
                    'merge_reason': merge_reason,
                    'primary_entity': _determine_primary_entity_standalone(entity1, entity2)
                })
    
    return candidates

def _calculate_similarity_scores_standalone(entity1: Dict, entity2: Dict, config: Dict) -> Dict[str, float]:
    """Standalone version of similarity calculation for parallel processing."""
    scores = {}
    
    title1 = entity1['title'].lower().strip()
    title2 = entity2['title'].lower().strip()
    
    # String similarity
    scores['string_similarity'] = _string_similarity_standalone(title1, title2)
    
    # Token overlap
    if config.get('enable_token_matching', True):
        scores['token_overlap'] = _token_overlap_similarity_standalone(title1, title2)
    else:
        scores['token_overlap'] = 0.0
    
    # Partial name matching
    if config.get('enable_partial_name_matching', True):
        scores['partial_name_match'] = _partial_name_similarity_standalone(title1, title2)
    else:
        scores['partial_name_match'] = 0.0
    
    # Abbreviation matching
    if config.get('enable_abbreviation_matching', True):
        scores['abbreviation_match'] = _abbreviation_similarity_standalone(title1, title2)
    else:
        scores['abbreviation_match'] = 0.0
    
    # Role-based matching
    if config.get('enable_role_based_matching', True):
        scores['role_match'] = _role_based_similarity_standalone(title1, title2)
    else:
        scores['role_match'] = 0.0
    
    # Graph structure similarity
    if config.get('enable_graph_structure_matching', True):
        scores['graph_structure'] = _graph_structure_similarity_standalone(entity1, entity2, config)
    else:
        scores['graph_structure'] = 0.0
    
    # Semantic similarity (simplified for parallel processing)
    scores['semantic_similarity'] = 0.0  # Skip for parallel version to avoid pickling issues
    
    return scores

def _string_similarity_standalone(str1: str, str2: str) -> float:
    """Standalone string similarity calculation."""
    if HAS_LEVENSHTEIN:
        lev_sim = 1 - (Levenshtein.distance(str1, str2) / max(len(str1), len(str2), 1))
    else:
        lev_sim = 0.0
    seq_sim = difflib.SequenceMatcher(None, str1, str2).ratio()
    return max(lev_sim, seq_sim)

def _token_overlap_similarity_standalone(str1: str, str2: str) -> float:
    """Standalone token overlap similarity calculation."""
    tokens1 = set(re.findall(r'\b\w+\b', str1.lower()))
    tokens2 = set(re.findall(r'\b\w+\b', str2.lower()))
    
    # Remove stop words
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    tokens1 = tokens1 - stop_words
    tokens2 = tokens2 - stop_words
    
    if not tokens1 or not tokens2:
        return 0.0
    
    intersection = tokens1.intersection(tokens2)
    union = tokens1.union(tokens2)
    
    if not union:
        return 0.0
    
    jaccard = len(intersection) / len(union)
    
    # Boost for subset matches
    if tokens1.issubset(tokens2) or tokens2.issubset(tokens1):
        min_tokens = min(len(tokens1), len(tokens2))
        if min_tokens >= 2 or (min_tokens == 1 and jaccard >= 0.5):
            return min(1.0, jaccard + 0.2)
    
    return jaccard

def _partial_name_similarity_standalone(str1: str, str2: str) -> float:
    """Standalone partial name similarity calculation."""
    tokens1 = re.findall(r'\b\w+\b', str1.lower())
    tokens2 = re.findall(r'\b\w+\b', str2.lower())
    
    if len(tokens1) <= len(tokens2):
        shorter, longer = tokens1, tokens2
    else:
        shorter, longer = tokens2, tokens1
    
    if not shorter:
        return 0.0
    
    matches = sum(1 for token in shorter if token in longer)
    return matches / len(shorter)

def _abbreviation_similarity_standalone(str1: str, str2: str) -> float:
    """Standalone abbreviation similarity calculation."""
    def get_initials(text):
        words = re.findall(r'\b\w+\b', text)
        return ''.join(word[0].lower() for word in words if word)
    
    def has_abbreviation_pattern(short, long):
        short_clean = re.sub(r'[^\w\s]', '', short).strip()
        long_clean = re.sub(r'[^\w\s]', '', long).strip()
        
        initials = get_initials(long_clean)
        if short_clean.replace('.', '').replace(' ', '').lower() == initials:
            return 1.0
        
        short_parts = short_clean.split()
        long_parts = long_clean.split()
        
        if len(short_parts) == len(long_parts):
            matches = 0
            for s_part, l_part in zip(short_parts, long_parts):
                s_clean = s_part.replace('.', '').lower()
                l_clean = l_part.lower()
                
                if s_clean == l_clean or (len(s_clean) == 1 and l_clean.startswith(s_clean)):
                    matches += 1
            
            return matches / len(short_parts)
        
        return 0.0
    
    score1 = has_abbreviation_pattern(str1, str2)
    score2 = has_abbreviation_pattern(str2, str1)
    return max(score1, score2)

def _role_based_similarity_standalone(str1: str, str2: str) -> float:
    """Standalone role-based similarity calculation."""
    roles = ['mayor', 'commissioner', 'councilman', 'councilwoman', 'director', 'manager', 'chief']
    
    def extract_role_and_name(text):
        text_lower = text.lower()
        for role in roles:
            if role in text_lower:
                name_part = text_lower.replace(role, '').strip()
                return role, name_part
        return None, text_lower
    
    role1, name1 = extract_role_and_name(str1)
    role2, name2 = extract_role_and_name(str2)
    
    if role1 and not role2:
        return _string_similarity_standalone(name1, str2)
    elif role2 and not role1:
        return _string_similarity_standalone(name2, str1)
    elif role1 and role2:
        if role1 == role2:
            return _string_similarity_standalone(name1, name2)
        else:
            return 0.0
    
    return 0.0

def _graph_structure_similarity_standalone(entity1: Dict, entity2: Dict, config: Dict) -> float:
    """Standalone graph structure similarity calculation."""
    neighbors1 = entity1.get('neighbors', set())
    neighbors2 = entity2.get('neighbors', set())
    
    if not neighbors1 and not neighbors2:
        return 1.0
    elif not neighbors1 or not neighbors2:
        return 0.0
    
    intersection = len(neighbors1.intersection(neighbors2))
    union = len(neighbors1.union(neighbors2))
    
    jaccard_sim = intersection / union if union > 0 else 0.0
    
    coeff1 = entity1.get('clustering_coeff', 0.0)
    coeff2 = entity2.get('clustering_coeff', 0.0)
    coeff_diff = abs(coeff1 - coeff2)
    coeff_sim = 1.0 - min(coeff_diff / config.get('clustering_tolerance', 0.15), 1.0)
    
    return (jaccard_sim + coeff_sim) / 2.0

def _calculate_combined_score_standalone(scores: Dict[str, float], weights: Dict[str, float]) -> float:
    """Standalone combined score calculation."""
    if scores.get('token_overlap', 0) > 0.7 and scores.get('string_similarity', 0) < 0.8:
        if scores.get('graph_structure', 0) < 0.6:
            return 0.0
    
    combined = 0.0
    total_weight = 0.0
    
    for score_type, weight in weights.items():
        if score_type in scores:
            combined += scores[score_type] * weight
            total_weight += weight
    
    bonus_scores = ['partial_name_match', 'abbreviation_match', 'role_match']
    max_bonus = 0.0
    for bonus_type in bonus_scores:
        if bonus_type in scores:
            max_bonus = max(max_bonus, scores[bonus_type])
    
    bonus_factor = min(max_bonus * 0.2, 0.2)
    
    if total_weight > 0:
        final_score = (combined / total_weight) + bonus_factor
        return min(final_score, 1.0)
    
    return 0.0

def _validate_merge_candidate_standalone(entity1: Dict, entity2: Dict, scores: Dict[str, float]) -> bool:
    """Standalone validation for merge candidates."""
    if scores.get('token_overlap', 0) > scores.get('string_similarity', 0):
        has_strong_evidence = (
            scores.get('graph_structure', 0) > 0.7 or
            scores.get('semantic_similarity', 0) > 0.8 or
            scores.get('abbreviation_match', 0) > 0.8 or
            scores.get('role_match', 0) > 0.7
        )
        
        if not has_strong_evidence:
            return False
    
    if entity1.get('description') and entity2.get('description'):
        desc1_len = len(str(entity1['description']))
        desc2_len = len(str(entity2['description']))
        
        if max(desc1_len, desc2_len) > 3 * min(desc1_len, desc2_len):
            return scores.get('combined_score', 0) > 0.9
    
    return True

def _determine_merge_reason_standalone(scores: Dict[str, float]) -> str:
    """Standalone merge reason determination."""
    main_scores = {
        'string_similarity': 'High string similarity',
        'token_overlap': 'Token overlap',
        'graph_structure': 'Similar graph structure',
        'semantic_similarity': 'Semantic similarity'
    }
    
    special_scores = {
        'partial_name_match': 'Partial name match',
        'abbreviation_match': 'Abbreviation pattern',
        'role_match': 'Role-based match'
    }
    
    for score_type, reason in special_scores.items():
        if scores.get(score_type, 0.0) > 0.7:
            return reason
    
    max_score = 0.0
    max_reason = "Combined similarity"
    
    for score_type, reason in main_scores.items():
        if scores.get(score_type, 0.0) > max_score:
            max_score = scores[score_type]
            max_reason = reason
    
    return max_reason

def _determine_primary_entity_standalone(entity1: Dict, entity2: Dict) -> str:
    """Standalone primary entity determination."""
    degree1 = entity1.get('degree_centrality', 0)
    degree2 = entity2.get('degree_centrality', 0)
    
    if degree1 > degree2:
        return entity1['title']
    elif degree2 > degree1:
        return entity2['title']
    
    if len(entity1['title']) > len(entity2['title']):
        return entity1['title']
    else:
        return entity2['title']

# Configuration presets
DEDUP_CONFIGS = {
    'aggressive': {
        'min_combined_score': 0.75,  # INCREASED from 0.65
        'enable_partial_name_matching': True,
        'enable_token_matching': True,
        'enable_semantic_matching': True,
        'enable_graph_structure_matching': True,
        'enable_abbreviation_matching': True,
        'enable_role_based_matching': True,
        'exact_match_threshold': 0.85,
        'high_similarity_threshold': 0.8,
        'partial_match_threshold': 0.6,
        'clustering_tolerance': 0.2,
        'weights': {
            'string_similarity': 0.2,
            'token_overlap': 0.2,
            'graph_structure': 0.4,  # Emphasize network evidence
            'semantic_similarity': 0.2
        }
    },
    'conservative': {
        'min_combined_score': 0.85,  # Already strict
        'enable_partial_name_matching': True,
        'enable_token_matching': True,
        'enable_semantic_matching': True,
        'enable_graph_structure_matching': True,
        'enable_abbreviation_matching': True,
        'enable_role_based_matching': False,
        'exact_match_threshold': 0.95,
        'high_similarity_threshold': 0.9,
        'partial_match_threshold': 0.8,
        'clustering_tolerance': 0.1,
        'weights': {
            'string_similarity': 0.4,
            'token_overlap': 0.1,    # Reduced to avoid false positives
            'graph_structure': 0.4,
            'semantic_similarity': 0.1
        }
    },
    'name_focused': {
        'min_combined_score': 0.8,   # INCREASED from 0.7
        'enable_partial_name_matching': True,
        'enable_token_matching': True,
        'enable_semantic_matching': True,
        'enable_graph_structure_matching': True,
        'enable_abbreviation_matching': True,
        'enable_role_based_matching': True,
        'exact_match_threshold': 0.95,
        'high_similarity_threshold': 0.85,
        'partial_match_threshold': 0.7,
        'clustering_tolerance': 0.15,
        'weights': {
            'string_similarity': 0.2,
            'token_overlap': 0.3,     # REDUCED from 0.4
            'graph_structure': 0.3,   # INCREASED from 0.2
            'semantic_similarity': 0.2
        }
    }
}


class EnhancedEntityDeduplicator:
    """
    Enhanced entity deduplication using multiple similarity metrics and advanced matching strategies.
    """
    
    def __init__(self, output_dir: Path, config: Dict[str, Any] = None):
        """
        Initialize the enhanced entity deduplicator.
        
        Args:
            output_dir: Path to GraphRAG output directory
            config: Configuration dictionary with matching strategies and thresholds
        """
        self.output_dir = Path(output_dir)
        self.config = config or DEDUP_CONFIGS['name_focused']
        
        # Initialize data containers
        self.entities_df = None
        self.relationships_df = None
        self.graph = None
        self.tfidf_vectorizer = None
        self.tfidf_matrix = None
        
        # Merge tracking
        self.merge_report = {
            'timestamp': datetime.now().isoformat(),
            'config': self.config.copy(),
            'merges': [],
            'statistics': {}
        }
        
        logger.info(f"Initialized enhanced deduplicator for {output_dir}")
        logger.info(f"Configuration: {self.config.get('min_combined_score', 0.7)} min score")
    
    def deduplicate_entities(self) -> Dict[str, Any]:
        """
        Main enhanced deduplication process.
        
        Returns:
            Dictionary with deduplication statistics
        """
        print("🚀 Starting Enhanced Entity Deduplication Process")
        print("=" * 60)
        logger.info("Starting enhanced entity deduplication process")
        
        # Load data
        print("\n📂 Step 1/6: Loading data...")
        self._load_data()
        original_entity_count = len(self.entities_df)
        print(f"   ✓ Loaded {original_entity_count:,} entities and {len(self.relationships_df):,} relationships")
        
        # Build graph and calculate features
        print("\n🏗️ Step 2/6: Building graph structure...")
        self.graph = self._build_graph(self.entities_df, self.relationships_df)
        
        print("\n📊 Step 3/6: Analyzing graph features...")
        self.entities_df = self._calculate_graph_features(self.entities_df)
        
        # Initialize semantic similarity if enabled
        if self.config.get('enable_semantic_matching', True) and HAS_SKLEARN:
            print("\n🧠 Step 4/6: Setting up semantic analysis...")
            self._initialize_semantic_similarity()
        else:
            print("\n⏭️ Step 4/6: Skipping semantic analysis (disabled or scikit-learn unavailable)")
        
        # Find merge candidates using enhanced strategies
        print("\n🔍 Step 5/6: Finding merge candidates...")
        merge_candidates = self._find_merge_candidates(self.entities_df, self.relationships_df)
        
        if merge_candidates:
            # Execute merges
            print("\n🔧 Step 6/6: Executing merges...")
            merged_entities_df = self._execute_merges(self.entities_df, merge_candidates)
        else:
            print("\n✨ Step 6/6: No merges needed - all entities are sufficiently distinct")
            merged_entities_df = self.entities_df
        
        # Save results
        print("\n💾 Saving results...")
        self._save_deduplicated_data(merged_entities_df)
        self._save_enhanced_report(merge_candidates)
        
        # Calculate statistics
        final_entity_count = len(merged_entities_df)
        merged_count = original_entity_count - final_entity_count
        
        stats = {
            'original_entities': original_entity_count,
            'merged_entities': final_entity_count,
            'merged_count': merged_count,
            'merge_candidates': len(merge_candidates),
            'output_dir': str(self.output_dir / "deduplicated"),
            'config_used': self.config
        }
        
        print("\n✅ Enhanced Deduplication Complete!")
        print("=" * 60)
        print(f"📊 Final Results:")
        print(f"   • Original entities: {original_entity_count:,}")
        print(f"   • Final entities: {final_entity_count:,}")
        print(f"   • Entities merged: {merged_count:,}")
        print(f"   • Reduction: {(merged_count/original_entity_count)*100:.1f}%")
        print(f"   • Output saved to: output/deduplicated/")
        
        logger.info(f"Enhanced deduplication complete: {original_entity_count} -> {final_entity_count} entities")
        return stats
    
    def _load_data(self):
        """Load GraphRAG entities and relationships data."""
        entities_path = self.output_dir / "entities.parquet"
        relationships_path = self.output_dir / "relationships.parquet"
        
        if not entities_path.exists():
            raise FileNotFoundError(f"Entities file not found: {entities_path}")
        if not relationships_path.exists():
            raise FileNotFoundError(f"Relationships file not found: {relationships_path}")
        
        self.entities_df = pd.read_parquet(entities_path)
        self.relationships_df = pd.read_parquet(relationships_path)
        
        logger.info(f"Loaded {len(self.entities_df)} entities and {len(self.relationships_df)} relationships")
    
    def _build_graph(self, entities_df: pd.DataFrame, relationships_df: pd.DataFrame) -> nx.Graph:
        """Build NetworkX graph from relationships for analysis."""
        graph = nx.Graph()
        
        # Add entities as nodes
        for _, entity in entities_df.iterrows():
            graph.add_node(entity['title'], **entity.to_dict())
        
        # Add relationships as edges
        for _, rel in relationships_df.iterrows():
            if 'source' in rel and 'target' in rel:
                graph.add_edge(rel['source'], rel['target'], **rel.to_dict())
        
        logger.info(f"Built graph with {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges")
        return graph
    
    def _calculate_graph_features(self, entities_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate graph-based features for entities."""
        df = entities_df.copy()
        
        print("🔍 Calculating graph features...")
        
        # Calculate clustering coefficients and degree centrality
        clustering_coeffs = {}
        degree_centrality = {}
        
        entity_names = df['title'].tolist()
        for entity_name in tqdm(entity_names, desc="Computing clustering & centrality", unit="entities"):
            if entity_name in self.graph:
                clustering_coeffs[entity_name] = nx.clustering(self.graph, entity_name)
                degree_centrality[entity_name] = self.graph.degree(entity_name)
            else:
                clustering_coeffs[entity_name] = 0.0
                degree_centrality[entity_name] = 0
        
        df['clustering_coeff'] = df['title'].map(clustering_coeffs)
        df['degree_centrality'] = df['title'].map(degree_centrality)
        
        # Calculate neighbor sets for structure comparison
        neighbor_sets = {}
        for entity_name in tqdm(entity_names, desc="Building neighbor sets", unit="entities"):
            if entity_name in self.graph:
                neighbor_sets[entity_name] = set(self.graph.neighbors(entity_name))
            else:
                neighbor_sets[entity_name] = set()
        
        df['neighbors'] = df['title'].map(neighbor_sets)
        
        logger.info("Calculated graph features for all entities")
        return df
    
    def _initialize_semantic_similarity(self):
        """Initialize TF-IDF vectorizer for semantic similarity."""
        if not HAS_SKLEARN:
            logger.warning("scikit-learn not available, skipping semantic similarity")
            return
        
        print("🧠 Initializing semantic similarity analysis...")
        
        # Prepare text corpus from entity descriptions
        descriptions = []
        for _, entity in tqdm(self.entities_df.iterrows(), desc="Preparing text data", 
                             total=len(self.entities_df), unit="entities"):
            desc = entity.get('description', '') or ''
            title = entity.get('title', '') or ''
            combined_text = f"{title} {desc}".strip()
            descriptions.append(combined_text if combined_text else title)
        
        # Initialize TF-IDF vectorizer
        print("📊 Fitting TF-IDF vectorizer...")
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            lowercase=True,
            ngram_range=(1, 2)
        )
        
        try:
            self.tfidf_matrix = self.tfidf_vectorizer.fit_transform(descriptions)
            logger.info(f"Initialized semantic similarity with {self.tfidf_matrix.shape[1]} features")
        except Exception as e:
            logger.warning(f"Failed to initialize semantic similarity: {e}")
            self.tfidf_matrix = None
    
    def _find_merge_candidates(self, entities_df: pd.DataFrame, relationships_df: pd.DataFrame) -> List[Dict]:
        """Find entity merge candidates using enhanced matching strategies with optimized processing."""
        entity_list = entities_df.to_dict('records')
        total_comparisons = len(entity_list) * (len(entity_list) - 1) // 2
        
        print(f"\n🔍 Analyzing {total_comparisons:,} entity pairs for potential merges...")
        
        # Use optimized sequential processing with better chunking
        return self._find_merge_candidates_optimized(entity_list, total_comparisons)
    
    def _process_entity_chunk(self, entity_pairs: List[Tuple[Dict, Dict]]) -> List[Dict]:
        """Process a chunk of entity pairs using instance methods."""
        candidates = []
        
        for entity1, entity2 in entity_pairs:
            # Calculate similarity scores
            scores = self._calculate_similarity_scores(entity1, entity2)
            
            # Calculate combined score
            combined_score = self._calculate_combined_score(scores)
            
            # Check threshold
            if combined_score >= self.config.get('min_combined_score', 0.7):
                # Validate candidate
                scores['combined_score'] = combined_score
                if self._validate_merge_candidate(entity1, entity2, scores):
                    # Determine merge reason
                    merge_reason = self._determine_merge_reason(scores)
                    
                    candidates.append({
                        'entity1_title': entity1['title'],
                        'entity2_title': entity2['title'],
                        'entity1_id': entity1.get('id', entity1.get('human_readable_id', '')),
                        'entity2_id': entity2.get('id', entity2.get('human_readable_id', '')),
                        'combined_score': combined_score,
                        'individual_scores': scores,
                        'merge_reason': merge_reason,
                        'primary_entity': self._determine_primary_entity(entity1, entity2)
                    })
        
        return candidates
    
    def _find_merge_candidates_optimized(self, entity_list: List[Dict], total_comparisons: int) -> List[Dict]:
        """Optimized version with smart filtering and progress tracking."""
        candidates = []
        
        print("   Using optimized processing with smart filtering")
        
        # First pass: Quick filtering based on simple criteria
        print("   📋 Pre-filtering entities for faster processing...")
        
        # Group entities by first letter and length for quick filtering
        entity_groups = {}
        for i, entity in enumerate(entity_list):
            title = entity['title'].lower().strip()
            # Group by first letter and rough length category
            key = (title[0] if title else '', len(title) // 5)  # Length buckets of 5
            if key not in entity_groups:
                entity_groups[key] = []
            entity_groups[key].append((i, entity))
        
        # Only compare entities within similar groups or with high connectivity
        comparison_pairs = []
        for group_entities in entity_groups.values():
            # Within-group comparisons (more likely to match)
            for i, (idx1, entity1) in enumerate(group_entities):
                for j, (idx2, entity2) in enumerate(group_entities[i+1:], i+1):
                    comparison_pairs.append((entity1, entity2))
        
        # Add cross-group comparisons for highly connected entities
        high_connectivity_threshold = 5  # entities with 5+ connections
        high_connectivity_entities = [
            entity for entity in entity_list 
            if entity.get('degree_centrality', 0) >= high_connectivity_threshold
        ]
        
        # Cross-group comparisons for high-connectivity entities
        for i, entity1 in enumerate(high_connectivity_entities):
            for entity2 in high_connectivity_entities[i+1:]:
                # Avoid duplicates
                pair_exists = any(
                    (p[0]['title'] == entity1['title'] and p[1]['title'] == entity2['title']) or
                    (p[0]['title'] == entity2['title'] and p[1]['title'] == entity1['title'])
                    for p in comparison_pairs
                )
                if not pair_exists:
                    comparison_pairs.append((entity1, entity2))
        
        actual_comparisons = len(comparison_pairs)
        reduction_percent = (1 - actual_comparisons/total_comparisons) * 100
        
        print(f"   ✂️ Reduced from {total_comparisons:,} to {actual_comparisons:,} comparisons ({reduction_percent:.1f}% reduction)")
        
        # Process the filtered comparisons with progress
        processed = 0
        update_frequency = max(1, actual_comparisons // 100)  # Update every 1%
        
        with tqdm(total=actual_comparisons, desc="Comparing entity pairs", unit="pairs") as pbar:
            for entity1, entity2 in comparison_pairs:
                # Calculate similarity scores
                scores = self._calculate_similarity_scores(entity1, entity2)
                
                # Calculate combined score
                combined_score = self._calculate_combined_score(scores)
                
                # Check if it meets the threshold
                if combined_score >= self.config.get('min_combined_score', 0.7):
                    # Additional validation
                    scores['combined_score'] = combined_score
                    if self._validate_merge_candidate(entity1, entity2, scores):
                        # Determine merge reason
                        merge_reason = self._determine_merge_reason(scores)
                        
                        candidates.append({
                            'entity1_title': entity1['title'],
                            'entity2_title': entity2['title'],
                            'entity1_id': entity1.get('id', entity1.get('human_readable_id', '')),
                            'entity2_id': entity2.get('id', entity2.get('human_readable_id', '')),
                            'combined_score': combined_score,
                            'individual_scores': scores,
                            'merge_reason': merge_reason,
                            'primary_entity': self._determine_primary_entity(entity1, entity2)
                        })
                
                processed += 1
                if processed % update_frequency == 0:
                    pbar.update(update_frequency)
                    pbar.set_description(f"Comparing pairs (found {len(candidates)} candidates)")
            
            # Update any remaining
            pbar.update(actual_comparisons - pbar.n)
        
        # Sort by combined score (highest first)
        candidates.sort(key=lambda x: x['combined_score'], reverse=True)
        
        logger.info(f"Found {len(candidates)} merge candidates using optimized processing")
        return candidates
    
    def _find_merge_candidates_sequential(self, entity_list: List[Dict]) -> List[Dict]:
        """Sequential version for smaller datasets or fallback."""
        candidates = []
        processed_pairs = set()
        total_comparisons = len(entity_list) * (len(entity_list) - 1) // 2
        
        comparisons_made = 0
        with tqdm(total=total_comparisons, desc="Comparing entity pairs", unit="pairs") as pbar:
            for i, entity1 in enumerate(entity_list):
                for j, entity2 in enumerate(entity_list[i+1:], i+1):
                    pair_key = tuple(sorted([entity1['title'], entity2['title']]))
                    if pair_key in processed_pairs:
                        continue
                    processed_pairs.add(pair_key)
                    
                    # Calculate multiple similarity scores
                    scores = self._calculate_similarity_scores(entity1, entity2)
                    
                    # Combine scores using weighted average
                    combined_score = self._calculate_combined_score(scores)
                    
                    # Check if it meets the threshold
                    if combined_score >= self.config.get('min_combined_score', 0.7):
                        # Additional validation
                        scores['combined_score'] = combined_score
                        if self._validate_merge_candidate(entity1, entity2, scores):
                            # Determine merge reason
                            merge_reason = self._determine_merge_reason(scores)
                            
                            candidates.append({
                                'entity1_title': entity1['title'],
                                'entity2_title': entity2['title'],
                                'entity1_id': entity1.get('id', i),
                                'entity2_id': entity2.get('id', j),
                                'combined_score': combined_score,
                                'individual_scores': scores,
                                'merge_reason': merge_reason,
                                'primary_entity': self._determine_primary_entity(entity1, entity2)
                            })
                    
                    comparisons_made += 1
                    pbar.update(1)
                    
                    # Update description with current progress stats
                    if comparisons_made % 10000 == 0:
                        pbar.set_description(f"Comparing pairs (found {len(candidates)} candidates)")
        
        # Sort by combined score (highest first)
        candidates.sort(key=lambda x: x['combined_score'], reverse=True)
        
        logger.info(f"Found {len(candidates)} merge candidates using sequential processing")
        return candidates
    
    def _calculate_similarity_scores(self, entity1: Dict, entity2: Dict) -> Dict[str, float]:
        """Calculate multiple similarity scores between two entities."""
        scores = {}
        
        title1 = entity1['title'].lower().strip()
        title2 = entity2['title'].lower().strip()
        
        # 1. String similarity
        scores['string_similarity'] = self._string_similarity(title1, title2)
        
        # 2. Token overlap
        if self.config.get('enable_token_matching', True):
            scores['token_overlap'] = self._token_overlap_similarity(title1, title2)
        else:
            scores['token_overlap'] = 0.0
        
        # 3. Partial name matching
        if self.config.get('enable_partial_name_matching', True):
            scores['partial_name_match'] = self._partial_name_similarity(title1, title2)
        else:
            scores['partial_name_match'] = 0.0
        
        # 4. Abbreviation matching
        if self.config.get('enable_abbreviation_matching', True):
            scores['abbreviation_match'] = self._abbreviation_similarity(title1, title2)
        else:
            scores['abbreviation_match'] = 0.0
        
        # 5. Role-based matching
        if self.config.get('enable_role_based_matching', True):
            scores['role_match'] = self._role_based_similarity(title1, title2)
        else:
            scores['role_match'] = 0.0
        
        # 6. Graph structure similarity
        if self.config.get('enable_graph_structure_matching', True):
            scores['graph_structure'] = self._graph_structure_similarity(entity1, entity2)
        else:
            scores['graph_structure'] = 0.0
        
        # 7. Semantic similarity
        if self.config.get('enable_semantic_matching', True) and self.tfidf_matrix is not None:
            scores['semantic_similarity'] = self._semantic_similarity(entity1, entity2)
        else:
            scores['semantic_similarity'] = 0.0
        
        return scores
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """Calculate string similarity using multiple methods."""
        if HAS_LEVENSHTEIN:
            # Use Levenshtein distance if available
            lev_sim = 1 - (Levenshtein.distance(str1, str2) / max(len(str1), len(str2), 1))
        else:
            lev_sim = 0.0
        
        # SequenceMatcher similarity
        seq_sim = difflib.SequenceMatcher(None, str1, str2).ratio()
        
        # Return the maximum of available similarities
        return max(lev_sim, seq_sim)
    
    def _token_overlap_similarity(self, str1: str, str2: str) -> float:
        """Calculate token overlap score with stricter criteria."""
        tokens1 = set(self._tokenize_and_clean(str1))
        tokens2 = set(self._tokenize_and_clean(str2))
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1.intersection(tokens2)
        union = tokens1.union(tokens2)
        
        if not union:
            return 0.0
        
        jaccard = len(intersection) / len(union)
        
        # STRICTER: Only boost if one is complete subset AND it's a meaningful match
        if tokens1.issubset(tokens2) or tokens2.issubset(tokens1):
            # Require at least 2 tokens in the subset for meaningful match
            min_tokens = min(len(tokens1), len(tokens2))
            if min_tokens >= 2 or (min_tokens == 1 and jaccard >= 0.5):
                return min(1.0, jaccard + 0.2)  # Reduced boost from 0.3
        
        return jaccard
    
    def _tokenize_and_clean(self, text: str) -> set:
        """Helper method to tokenize and clean text."""
        # Tokenize and clean
        tokens = set(re.findall(r'\b\w+\b', text.lower()))
        
        # Remove common stop words
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        return tokens - stop_words
    
    def _partial_name_similarity(self, str1: str, str2: str) -> float:
        """Check if one name is a partial match of another."""
        tokens1 = re.findall(r'\b\w+\b', str1.lower())
        tokens2 = re.findall(r'\b\w+\b', str2.lower())
        
        # Check if all tokens of shorter name are in longer name
        if len(tokens1) <= len(tokens2):
            shorter, longer = tokens1, tokens2
        else:
            shorter, longer = tokens2, tokens1
        
        if not shorter:
            return 0.0
        
        matches = sum(1 for token in shorter if token in longer)
        return matches / len(shorter)
    
    def _abbreviation_similarity(self, str1: str, str2: str) -> float:
        """Check for abbreviation patterns."""
        # Extract initials and check against full names
        def get_initials(text):
            words = re.findall(r'\b\w+\b', text)
            return ''.join(word[0].lower() for word in words if word)
        
        def has_abbreviation_pattern(short, long):
            # Check if short form could be abbreviation of long form
            short_clean = re.sub(r'[^\w\s]', '', short).strip()
            long_clean = re.sub(r'[^\w\s]', '', long).strip()
            
            # Check initials
            initials = get_initials(long_clean)
            if short_clean.replace('.', '').replace(' ', '').lower() == initials:
                return 1.0
            
            # Check partial abbreviations (e.g., "V. Lago" vs "Vince Lago")
            short_parts = short_clean.split()
            long_parts = long_clean.split()
            
            if len(short_parts) == len(long_parts):
                matches = 0
                for s_part, l_part in zip(short_parts, long_parts):
                    s_clean = s_part.replace('.', '').lower()
                    l_clean = l_part.lower()
                    
                    if s_clean == l_clean or (len(s_clean) == 1 and l_clean.startswith(s_clean)):
                        matches += 1
                
                return matches / len(short_parts)
            
            return 0.0
        
        # Check both directions
        score1 = has_abbreviation_pattern(str1, str2)
        score2 = has_abbreviation_pattern(str2, str1)
        
        return max(score1, score2)
    
    def _role_based_similarity(self, str1: str, str2: str) -> float:
        """Check for role-based matches."""
        roles = ['mayor', 'commissioner', 'councilman', 'councilwoman', 'director', 'manager', 'chief']
        
        def extract_role_and_name(text):
            text_lower = text.lower()
            for role in roles:
                if role in text_lower:
                    # Extract the name part
                    name_part = text_lower.replace(role, '').strip()
                    return role, name_part
            return None, text_lower
        
        role1, name1 = extract_role_and_name(str1)
        role2, name2 = extract_role_and_name(str2)
        
        # If one has a role and the other doesn't, check if names match
        if role1 and not role2:
            return self._string_similarity(name1, str2)
        elif role2 and not role1:
            return self._string_similarity(name2, str1)
        elif role1 and role2:
            # Both have roles, check if they match and names are similar
            if role1 == role2:
                return self._string_similarity(name1, name2)
            else:
                return 0.0
        
        return 0.0
    
    def _graph_structure_similarity(self, entity1: Dict, entity2: Dict) -> float:
        """Calculate graph structure similarity."""
        # Get neighbors from pre-calculated neighbor sets
        neighbors1 = entity1.get('neighbors', set())
        neighbors2 = entity2.get('neighbors', set())
        
        if not neighbors1 and not neighbors2:
            return 1.0  # Both isolated
        elif not neighbors1 or not neighbors2:
            return 0.0  # One isolated, one connected
        
        # Jaccard similarity of neighbor sets
        intersection = len(neighbors1.intersection(neighbors2))
        union = len(neighbors1.union(neighbors2))
        
        jaccard_sim = intersection / union if union > 0 else 0.0
        
        # Also compare clustering coefficients
        coeff1 = entity1.get('clustering_coeff', 0.0)
        coeff2 = entity2.get('clustering_coeff', 0.0)
        coeff_diff = abs(coeff1 - coeff2)
        coeff_sim = 1.0 - min(coeff_diff / self.config.get('clustering_tolerance', 0.15), 1.0)
        
        return (jaccard_sim + coeff_sim) / 2.0
    
    def _semantic_similarity(self, entity1: Dict, entity2: Dict) -> float:
        """Calculate semantic similarity using TF-IDF."""
        if self.tfidf_matrix is None:
            return 0.0
        
        try:
            # Get entity indices (this is a simplified approach)
            entity1_idx = None
            entity2_idx = None
            
            for idx, row in self.entities_df.iterrows():
                if row['title'] == entity1['title']:
                    entity1_idx = idx
                elif row['title'] == entity2['title']:
                    entity2_idx = idx
                
                if entity1_idx is not None and entity2_idx is not None:
                    break
            
            if entity1_idx is None or entity2_idx is None:
                return 0.0
            
            # Calculate cosine similarity
            vec1 = self.tfidf_matrix[entity1_idx:entity1_idx+1]
            vec2 = self.tfidf_matrix[entity2_idx:entity2_idx+1]
            
            similarity = cosine_similarity(vec1, vec2)[0, 0]
            return similarity
        except Exception as e:
            logger.debug(f"Error calculating semantic similarity: {e}")
            return 0.0
    
    def _calculate_combined_score(self, scores: Dict[str, float]) -> float:
        """Combine scores with strict requirements for partial matches."""
        # For partial name matches, require strong corroborating evidence
        if scores.get('token_overlap', 0) > 0.7 and scores.get('string_similarity', 0) < 0.8:
            # This is likely a partial name match (e.g., "Lago" vs "Vince Lago")
            # Require high neighbor overlap as confirmation
            if scores.get('graph_structure', 0) < 0.6:
                return 0.0  # Reject without strong network evidence
        
        # Original weighted combination
        weights = self.config.get('weights', {
            'string_similarity': 0.2,
            'token_overlap': 0.4,
            'graph_structure': 0.2,
            'semantic_similarity': 0.2
        })
        
        # Combine main scores
        combined = 0.0
        total_weight = 0.0
        
        for score_type, weight in weights.items():
            if score_type in scores:
                combined += scores[score_type] * weight
                total_weight += weight
        
        # Add bonus scores for special matches
        bonus_scores = ['partial_name_match', 'abbreviation_match', 'role_match']
        max_bonus = 0.0
        for bonus_type in bonus_scores:
            if bonus_type in scores:
                max_bonus = max(max_bonus, scores[bonus_type])
        
        # Apply bonus (up to 20% boost)
        bonus_factor = min(max_bonus * 0.2, 0.2)
        
        if total_weight > 0:
            final_score = (combined / total_weight) + bonus_factor
            return min(final_score, 1.0)
        
        return 0.0
    
    def _determine_merge_reason(self, scores: Dict[str, float]) -> str:
        """Determine the primary reason for merging."""
        # Find the highest contributing factor
        main_scores = {
            'string_similarity': 'High string similarity',
            'token_overlap': 'Token overlap',
            'graph_structure': 'Similar graph structure',
            'semantic_similarity': 'Semantic similarity'
        }
        
        special_scores = {
            'partial_name_match': 'Partial name match',
            'abbreviation_match': 'Abbreviation pattern',
            'role_match': 'Role-based match'
        }
        
        # Check special patterns first
        for score_type, reason in special_scores.items():
            if scores.get(score_type, 0.0) > 0.7:
                return reason
        
        # Find highest main score
        max_score = 0.0
        max_reason = "Combined similarity"
        
        for score_type, reason in main_scores.items():
            if scores.get(score_type, 0.0) > max_score:
                max_score = scores[score_type]
                max_reason = reason
        
        return max_reason
    
    def _determine_primary_entity(self, entity1: Dict, entity2: Dict) -> str:
        """Determine which entity should be kept as primary."""
        # Prefer entity with higher degree centrality
        degree1 = entity1.get('degree_centrality', 0)
        degree2 = entity2.get('degree_centrality', 0)
        
        if degree1 > degree2:
            return entity1['title']
        elif degree2 > degree1:
            return entity2['title']
        
        # If same degree, prefer longer/more descriptive name
        if len(entity1['title']) > len(entity2['title']):
            return entity1['title']
        else:
            return entity2['title']
    
    def _validate_merge_candidate(self, entity1: Dict, entity2: Dict, 
                                scores: Dict[str, float]) -> bool:
        """Additional validation to prevent false positives."""
        # Special validation for partial name matches
        if scores.get('token_overlap', 0) > scores.get('string_similarity', 0):
            # This suggests a partial match like "Lago" vs "Vince Lago"
            
            # Require either:
            # 1. Very high neighbor overlap (>70%)
            # 2. High semantic similarity (>80%) 
            # 3. Abbreviation or role match
            
            has_strong_evidence = (
                scores.get('graph_structure', 0) > 0.7 or
                scores.get('semantic_similarity', 0) > 0.8 or
                scores.get('abbreviation_match', 0) > 0.8 or
                scores.get('role_match', 0) > 0.7
            )
            
            if not has_strong_evidence:
                return False
        
        # Prevent merging entities with very different descriptions
        if entity1.get('description') and entity2.get('description'):
            desc1_len = len(str(entity1['description']))
            desc2_len = len(str(entity2['description']))
            
            # If descriptions differ significantly in length, be more cautious
            if max(desc1_len, desc2_len) > 3 * min(desc1_len, desc2_len):
                # Require higher combined score for very different descriptions
                return scores.get('combined_score', 0) > 0.9
        
        return True
    
    def _execute_merges(self, entities_df: pd.DataFrame, merge_candidates: List[Dict]) -> pd.DataFrame:
        """Execute the entity merges."""
        merged_df = entities_df.copy()
        entities_to_remove = set()
        merge_map = {}  # Maps old entity -> new entity
        
        # Add aliases column if it doesn't exist
        if 'aliases' not in merged_df.columns:
            merged_df['aliases'] = ''
        
        print(f"\n🔧 Executing {len(merge_candidates)} entity merges...")
        
        for candidate in tqdm(merge_candidates, desc="Executing merges", unit="merges"):
            entity1_title = candidate['entity1_title']
            entity2_title = candidate['entity2_title']
            primary_entity = candidate['primary_entity']
            
            # Skip if either entity was already merged
            if entity1_title in entities_to_remove or entity2_title in entities_to_remove:
                continue
            
            # Determine which entity to remove
            if primary_entity == entity1_title:
                keep_entity = entity1_title
                remove_entity = entity2_title
            else:
                keep_entity = entity2_title
                remove_entity = entity1_title
            
            # Update the primary entity
            primary_mask = merged_df['title'] == keep_entity
            remove_mask = merged_df['title'] == remove_entity
            
            if primary_mask.any() and remove_mask.any():
                primary_row = merged_df[primary_mask].iloc[0]
                remove_row = merged_df[remove_mask].iloc[0]
                
                # Merge descriptions
                primary_desc = primary_row.get('description', '') or ''
                remove_desc = remove_row.get('description', '') or ''
                
                if remove_desc and remove_desc not in primary_desc:
                    merged_desc = f"{primary_desc}\n[Also known as: {remove_entity}] {remove_desc}".strip()
                else:
                    merged_desc = f"{primary_desc}\n[Also known as: {remove_entity}]".strip()
                
                # Update aliases
                existing_aliases = primary_row.get('aliases', '') or ''
                if existing_aliases:
                    new_aliases = f"{existing_aliases}|{remove_entity}"
                else:
                    new_aliases = remove_entity
                
                # Apply updates
                merged_df.loc[primary_mask, 'description'] = merged_desc
                merged_df.loc[primary_mask, 'aliases'] = new_aliases
                
                # Mark for removal
                entities_to_remove.add(remove_entity)
                merge_map[remove_entity] = keep_entity
                
                # Record merge
                self.merge_report['merges'].append({
                    'primary_entity': keep_entity,
                    'merged_entity': remove_entity,
                    'combined_score': candidate['combined_score'],
                    'merge_reason': candidate['merge_reason'],
                    'individual_scores': candidate['individual_scores']
                })
        
        # Remove merged entities
        merged_df = merged_df[~merged_df['title'].isin(entities_to_remove)]
        
        logger.info(f"Executed {len(entities_to_remove)} merges")
        return merged_df
    
    def _save_deduplicated_data(self, merged_entities_df: pd.DataFrame):
        """Save deduplicated data to output directory."""
        output_subdir = self.output_dir / "deduplicated"
        output_subdir.mkdir(exist_ok=True)
        
        # Save merged entities
        entities_output = output_subdir / "entities.parquet"
        merged_entities_df.to_parquet(entities_output, index=False)
        
        # Copy other files unchanged
        other_files = [
            "relationships.parquet",
            "communities.parquet", 
            "community_reports.parquet"
        ]
        
        for filename in other_files:
            source_path = self.output_dir / filename
            target_path = output_subdir / filename
            
            if source_path.exists():
                import shutil
                shutil.copy2(source_path, target_path)
        
        logger.info(f"Saved deduplicated data to {output_subdir}")
    
    def _save_enhanced_report(self, merge_candidates: List[Dict]):
        """Save detailed enhanced deduplication report."""
        # Save JSON report
        report_path = self.output_dir / "enhanced_deduplication_report.json"
        self.merge_report['statistics'] = {
            'total_candidates': len(merge_candidates),
            'executed_merges': len(self.merge_report['merges']),
            'config_used': self.config
        }
        
        with open(report_path, 'w') as f:
            json.dump(self.merge_report, f, indent=2)
        
        # Save human-readable text report
        text_report_path = self.output_dir / "enhanced_deduplication_report.txt"
        with open(text_report_path, 'w') as f:
            f.write("Enhanced Entity Deduplication Report\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Timestamp: {self.merge_report['timestamp']}\n")
            f.write(f"Configuration: {self.config.get('min_combined_score', 0.7)} min score\n")
            f.write(f"Total candidates found: {len(merge_candidates)}\n")
            f.write(f"Merges executed: {len(self.merge_report['merges'])}\n\n")
            
            f.write("Executed Merges:\n")
            f.write("-" * 20 + "\n")
            
            for merge in self.merge_report['merges']:
                f.write(f"\n'{merge['primary_entity']}' ← '{merge['merged_entity']}'\n")
                f.write(f"  Score: {merge['combined_score']:.3f}\n")
                f.write(f"  Reason: {merge['merge_reason']}\n")
                f.write(f"  Details: {merge['individual_scores']}\n")
            
            if merge_candidates:
                f.write(f"\n\nTop candidates (not merged):\n")
                f.write("-" * 30 + "\n")
                
                # Show top 10 candidates that weren't merged
                unmerged = [c for c in merge_candidates 
                           if not any(m['merged_entity'] in [c['entity1_title'], c['entity2_title']] 
                                     for m in self.merge_report['merges'])]
                
                for candidate in unmerged[:10]:
                    f.write(f"\n'{candidate['entity1_title']}' ↔ '{candidate['entity2_title']}'\n")
                    f.write(f"  Score: {candidate['combined_score']:.3f}\n")
                    f.write(f"  Reason: {candidate['merge_reason']}\n")
        
        logger.info(f"Saved enhanced deduplication report to {text_report_path}")


# Main execution for standalone testing
if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path
    
    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.append(str(project_root))
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Run enhanced entity deduplication')
    parser.add_argument('--output-dir', '-o', type=str, default='graphrag_data/output',
                       help='Path to GraphRAG output directory')
    parser.add_argument('--config', '-c', choices=['aggressive', 'conservative', 'name_focused'],
                       default='name_focused', help='Deduplication configuration preset')
    parser.add_argument('--min-score', type=float, help='Override minimum combined score')
    
    args = parser.parse_args()
    
    output_dir = project_root / args.output_dir
    
    if not output_dir.exists():
        print(f"❌ Output directory not found: {output_dir}")
        sys.exit(1)
    
    # Get configuration
    config = DEDUP_CONFIGS.get(args.config, DEDUP_CONFIGS['name_focused']).copy()
    if args.min_score:
        config['min_combined_score'] = args.min_score
    
    print(f"🔍 Running enhanced entity deduplication on {output_dir}")
    print(f"   Configuration: {args.config}")
    print(f"   Min combined score: {config['min_combined_score']}")
    
    try:
        deduplicator = EnhancedEntityDeduplicator(output_dir, config)
        stats = deduplicator.deduplicate_entities()
        
        print(f"\n✅ Enhanced deduplication complete!")
        print(f"   Original entities: {stats['original_entities']}")
        print(f"   After deduplication: {stats['merged_entities']}")
        print(f"   Entities merged: {stats['merged_count']}")
        print(f"   Merge candidates: {stats['merge_candidates']}")
        print(f"   Output saved to: {stats['output_dir']}")
        
    except Exception as e:
        print(f"❌ Enhanced deduplication failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 