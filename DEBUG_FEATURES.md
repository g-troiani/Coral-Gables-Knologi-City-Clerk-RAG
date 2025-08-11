# Debug Features for Document Flow and Relationship Linking Issues

## Overview

This document describes the comprehensive debugging features added to track document loss and relationship linking failures in the City Clerk Knowledge Graph Pipeline.

## Debug Flags

All debugging is controlled by boolean flags in `scripts/graph_rag_stages/main_pipeline.py`:

```python
# --- DEBUG FLAGS ---
DEBUG_DOCUMENT_FLOW = False        # Enable detailed document flow tracing
DEBUG_RELATIONSHIP_LINKING = False # Enable relationship linking debugging  
DEBUG_ENTITY_DEDUPLICATION = False # Enable entity deduplication debugging
DEBUG_FILE_DISCOVERY = False       # Enable file discovery debugging
```

## Quick Enable/Disable

Use the provided `enable_debug.py` script:

```bash
# Enable all debugging
python enable_debug.py

# Disable all debugging  
python enable_debug.py disable
```

## Debug Features by Stage

### 🔍 STAGE 1: Data Pre-processing & Extraction

**Main Pipeline (main_pipeline.py)**
- ✅ Input PDF count before extraction
- ✅ Output JSON count after extraction
- ✅ File discovery in extracted JSON directory
- ✅ Subdirectory structure validation

**Debug Output:**
```
🔍 DEBUG [STAGE 1 INPUT] TOTAL: X source PDFs
🔍 DEBUG [STAGE 1 OUTPUT] TOTAL: Y extracted JSON files
🔍 DEBUG [STAGE 1 OUTPUT] stage1/: N JSON files
🔍 DEBUG [STAGE 1 OUTPUT] legal/: N JSON files
```

### 🔍 STAGE 1.5: JSON-to-Markdown Conversion

**JSON-to-Markdown Converter (json_to_markdown_converter.py)**
- ✅ Pre-conversion JSON file discovery
- ✅ Per-directory file counting
- ✅ Individual file conversion tracking
- ✅ PDF group processing details
- ✅ **CRITICAL LOSS DETECTION** - Alerts when documents are dropped

**Debug Output:**
```
🔍 DEBUG [JSON-TO-MD] Discovered X PDF groups
🔍 DEBUG [JSON-TO-MD] Total JSON files found: Y
🔍 DEBUG [JSON-TO-MD] Processing PDF group 'agenda_01_09_2024' with Z files
🔍 DEBUG [JSON-TO-MD] ✅ Successfully converted: file.md
🚨 CRITICAL: STAGE 1.5 DOCUMENT LOSS DETECTED!
🚨   Input JSON files: 8
🚨   Output MD files: 6
🚨   LOST: 2 documents
```

**File Discovery Details:**
```
🔍 DEBUG [FILE-DISCOVERY] Checking legal/ with pattern *_enhanced_*.json
🔍 DEBUG [FILE-DISCOVERY]   Found 3 files matching pattern
🔍 DEBUG [FILE-DISCOVERY]     Processing: 2024-02_-_01_09_2024_enhanced_ordinance.json
```

### 🔍 STAGE 2: NER Pipeline

**Main Pipeline (main_pipeline.py)**
- ✅ Pre-NER markdown file counting
- ✅ Fallback directory usage tracking

**Debug Output:**
```
🔍 DEBUG [STAGE 2 INPUT] TOTAL: X markdown files for NER
🔍 DEBUG [STAGE 2 FALLBACK] Using fallback directory for NER
```

### 🔍 STAGE 3: Taxonomy Synthesis

**Taxonomy Synthesizer (taxonomy_synthesizer.py)**
- ✅ JSON directory structure validation
- ✅ Subdirectory discovery and file counting
- ✅ **CRITICAL LEGAL DOCUMENT DISCOVERY** - Tracks the second major loss point
- ✅ Alternative pattern matching for troubleshooting

**Debug Output:**
```
🔍 DEBUG [TAXONOMY] Available subdirectories: ['agenda', 'legal', 'verbatim']
🔍 DEBUG [TAXONOMY] Found 3 legal documents matching pattern '*_enhanced_*.json'
🚨 CRITICAL: TAXONOMY LEGAL DISCOVERY ISSUE!
🚨   Expected more legal documents but only found: 3
🚨   Pattern used: '*_enhanced_*.json'
🔍 DEBUG [TAXONOMY]   Pattern '*.json': 5 files
🔍 DEBUG [TAXONOMY]   Pattern '*ordinance*': 2 files
```

### 🔍 STAGE 4: Entity Deduplication

**Entity Deduplicator Extended (entity_deduplicator_extended.py)**
- ✅ Multi-source entity loading tracking
- ✅ Before/after entity counts by type
- ✅ **RELATIONSHIP MERGING** - Critical for agenda-document links
- ✅ Relationship file discovery and loading

**Debug Output:**
```
🧹 DEBUG [DEDUPLICATION] Loaded 150 NER entities from 8 types
🧹 DEBUG [DEDUPLICATION]   Document: 25 entities
🧹 DEBUG [DEDUPLICATION]   AgendaItem: 15 entities
🔗 DEBUG [RELATIONSHIPS] Found 2 NER relationship files
🔗 DEBUG [RELATIONSHIPS] agenda_01_09_2024.json: 12 relationships
```

### 🔍 Stage Transitions

**Main Pipeline (main_pipeline.py)**
- ✅ Document count validation between stages
- ✅ **AUTOMATIC LOSS DETECTION** - Warns when document counts drop
- ✅ Stage-to-stage flow tracking

**Debug Output:**
```
🚦 DEBUG [TRANSITION] STAGE 1 → STAGE 1.5
🚦 DEBUG [TRANSITION]   JSON Input: 8 documents
🚦 DEBUG [TRANSITION]   Markdown Output: 6 documents
🚨 DEBUG [TRANSITION] POTENTIAL DATA LOSS: 2 documents missing
```

## Critical Loss Points Monitored

### 1. 🚨 STAGE 1.5: JSON-to-Markdown Conversion
- **Issue**: Documents lost during conversion
- **Detection**: Input vs output count comparison
- **Debug**: File discovery patterns, conversion success/failure tracking

### 2. 🚨 STAGE 3: Taxonomy Legal Document Discovery  
- **Issue**: Legal documents not found for processing
- **Detection**: Pattern matching validation, alternative pattern testing
- **Debug**: Directory structure validation, file naming analysis

### 3. 🚨 STAGE 4: Entity Deduplication
- **Issue**: Legitimate relationships marked as duplicates and removed
- **Detection**: Relationship loading and merging tracking
- **Debug**: Before/after entity counts, relationship file processing

### 4. 🚨 STAGE 1: Document Linking
- **Issue**: Agenda items not properly linked to legal documents
- **Detection**: Relationship creation and persistence tracking
- **Debug**: Hierarchical linking success validation

## Usage Instructions

1. **Enable debugging**:
   ```bash
   python enable_debug.py
   ```

2. **Run the pipeline**:
   ```bash
   python -m scripts.graph_rag_stages.main_pipeline
   ```

3. **Monitor the logs** for debug messages with these prefixes:
   - `🔍 DEBUG [STAGE X]` - Stage-specific document counting
   - `🔍 DEBUG [JSON-TO-MD]` - JSON-to-Markdown conversion issues
   - `🔍 DEBUG [FILE-DISCOVERY]` - File discovery problems
   - `🔍 DEBUG [TAXONOMY]` - Taxonomy synthesis document loss
   - `🧹 DEBUG [DEDUPLICATION]` - Entity deduplication tracking
   - `🔗 DEBUG [RELATIONSHIPS]` - Relationship linking issues
   - `🚦 DEBUG [TRANSITION]` - Stage transition validation
   - `🚨 CRITICAL` - Critical loss detection alerts

4. **Disable debugging** when done:
   ```bash
   python enable_debug.py disable
   ```

## Expected Debug Output Flow

When debugging is enabled, you should see a detailed trace like:

```
🔍 DEBUG [STAGE 1 INPUT] TOTAL: 8 source PDFs
🔍 DEBUG [STAGE 1 OUTPUT] TOTAL: 8 extracted JSON files  
🔍 DEBUG [JSON-TO-MD] Discovered 3 PDF groups
🔍 DEBUG [JSON-TO-MD] Total JSON files found: 8
🚨 CRITICAL: STAGE 1.5 DOCUMENT LOSS DETECTED! LOST: 2 documents
🔍 DEBUG [TAXONOMY] Found 3 legal documents matching pattern
🚨 CRITICAL: TAXONOMY LEGAL DISCOVERY ISSUE! Expected more but found: 3
🧹 DEBUG [DEDUPLICATION] Before: 150, After: 125, Removed: 25
🔗 DEBUG [RELATIONSHIPS] Found 2 NER relationship files
```

This will help identify exactly where documents are being lost and why agenda-document relationships are not being established properly.
