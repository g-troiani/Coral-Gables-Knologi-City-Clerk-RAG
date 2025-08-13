"""
Vector Database Pusher - Pushes document chunks to Azure Cognitive Search with embeddings
"""

import json
import logging
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import hashlib
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SimpleField,
    SearchableField,
    SearchField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SemanticConfiguration,
    SemanticPrioritizedFields,
    SemanticField,
    SemanticSearch
)
from azure.core.credentials import AzureKeyCredential
from openai import AzureOpenAI
import os

log = logging.getLogger(__name__)


class VectorDatabasePusher:
    """Pushes document chunks with embeddings to Azure Cognitive Search."""
    
    def __init__(self, chunks_dir: Path, output_dir: Path):
        """Initialize the vector database pusher."""
        self.chunks_dir = chunks_dir
        self.output_dir = output_dir
        
        # Azure Cognitive Search configuration
        self.search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT", "").strip()
        self.search_key = os.getenv("VECTOR_DATABASE_KEY", "").strip()
        
        # New: allow a dedicated override just for vector uploads, and default to v3.
        # This avoids colliding with any readers that may still point at v2.
        VECTOR_INDEX_NAME = os.getenv("VECTOR_INDEX_NAME")
        self.index_name = VECTOR_INDEX_NAME or os.getenv("AZURE_SEARCH_INDEX_NAME", "city-clerk-rag-v3")
        
        log.info(
            "Index selection — VECTOR_INDEX_NAME=%r, AZURE_SEARCH_INDEX_NAME=%r, Effective=%s",
            os.getenv("VECTOR_INDEX_NAME"), os.getenv("AZURE_SEARCH_INDEX_NAME"), self.index_name
        )
        
        if not self.search_endpoint or not self.search_key:
            raise ValueError("Azure Search endpoint and key must be set in environment variables")
        
        # Initialize Azure OpenAI for embeddings
        self.openai_client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        # Embeddings model deployment name (you'll need to set this)
        # Prefer a modern default; keep dim configurable
        self.embeddings_model = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT", "").strip() or "text-embedding-3-small"
        if not os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT"):
            log.getLogger(__name__).warning(
                "AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT not set; defaulting to 'text-embedding-3-small'. "
                "Ensure VECTOR_DIM matches the model's dimension."
            )
        
        # Make vector dims configurable (prevents silent 400s when model changes)
        self.vector_dim = int(os.getenv("VECTOR_DIM", "1536"))
        
        # Initialize search clients
        self.index_client = SearchIndexClient(
            endpoint=self.search_endpoint,
            credential=AzureKeyCredential(self.search_key)
        )
        
        # Initialize search client using helper
        self._reset_search_client()
        
        # Batch settings
        self.batch_size = 100
        self.max_concurrent = 5
        
    def _reset_search_client(self):
        """Helper to recreate search client with current index name."""
        self.search_client = SearchClient(
            endpoint=self.search_endpoint,
            index_name=self.index_name,
            credential=AzureKeyCredential(self.search_key)
        )
        
    async def initialize_index(self):
        """Create or update the search index with the required schema."""
        log.info(f"Initializing search index: {self.index_name}")
        
        # Define the index schema based on your provided schema
        fields = [
            SimpleField(name="chunkKey", type="Edm.String", key=True),
            SearchableField(name="sourceDocument", type="Edm.String", 
                          filterable=True, sortable=True, facetable=True),
            SearchableField(name="content", type="Edm.String"),
            SearchField(
                name="vector",
                type="Collection(Edm.Single)",
                searchable=True,
                vector_search_dimensions=self.vector_dim,
                vector_search_profile_name="vector-profile-1"
            ),
            SimpleField(name="startPage", type="Edm.Int32", 
                       filterable=True, sortable=True, facetable=True),
            SimpleField(name="endPage", type="Edm.Int32", 
                       filterable=True, sortable=True, facetable=True),
            SearchableField(name="documentType", type="Edm.String", 
                          filterable=True, sortable=True, facetable=True),
            SimpleField(name="Date", type="Edm.DateTimeOffset", 
                       filterable=True, sortable=True, facetable=True)
        ]
        
        # Configure vector search
        vector_search = VectorSearch(
            algorithms=[
                HnswAlgorithmConfiguration(
                    name="hnsw-1",
                    parameters={
                        "m": 4,
                        "efConstruction": 400,
                        "efSearch": 500,
                        "metric": "cosine"
                    }
                )
            ],
            profiles=[
                VectorSearchProfile(
                    name="vector-profile-1",
                    algorithm_configuration_name="hnsw-1"
                )
            ]
        )
        
        # Configure semantic search
        semantic_config = SemanticConfiguration(
            name="sem",
            prioritized_fields=SemanticPrioritizedFields(
                content_fields=[SemanticField(field_name="content")],
                keywords_fields=[
                    SemanticField(field_name="sourceDocument"),
                    SemanticField(field_name="documentType")
                ]
            )
        )
        
        semantic_search = SemanticSearch(configurations=[semantic_config])
        
        # Create the index
        index = SearchIndex(
            name=self.index_name,
            fields=fields,
            vector_search=vector_search,
            semantic_search=semantic_search
        )
        
        try:
            # Try to create or update the index
            result = self.index_client.create_or_update_index(index)
            log.info(f"✅ Index '{self.index_name}' initialized successfully")
            return result
        except Exception as e:
            # Auto-fallback on schema conflicts (cannot delete existing fields)
            if "cannot be deleted" in str(e).lower():
                import time
                new_name = f"{self.index_name}-v{int(time.time())}"
                log.warning("Schema conflict on %s; retrying with %s", self.index_name, new_name)
                index.name = new_name
                result = self.index_client.create_or_update_index(index)
                self.index_name = new_name
                # IMPORTANT: point the data-plane client to the new index
                self._reset_search_client()
                
                # Smoke-check the data-plane binding
                try:
                    _ = self.search_client.search(search_text="*", top=1)
                    log.info(f"✅ Data-plane client verified for new index '{self.index_name}'")
                except Exception as smoke_error:
                    log.warning(f"⚠️ Smoke check failed for new index (may be empty): {smoke_error}")
                
                log.info(f"✅ Index '{self.index_name}' initialized successfully (after auto-fallback)")
                return result
            else:
                log.error(f"❌ Failed to initialize index: {e}")
                raise
    
    async def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text using Azure OpenAI."""
        try:
            response = self.openai_client.embeddings.create(
                model=self.embeddings_model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            log.error(f"Failed to generate embedding: {e}")
            raise
    
    def _read_chunk_file(self, chunk_file: Path) -> Dict[str, Any]:
        """Read and parse a chunk file using the existing format."""
        with open(chunk_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        chunk_data = {
            "file_path": str(chunk_file),
            "file_name": chunk_file.name
        }
        
        if "---" in content:
            header, text = content.split("---", 1)
            
            # Parse header metadata (matching your existing format)
            for line in header.strip().split("\n"):
                if line.startswith("#") and ":" in line:
                    key_value = line[1:].strip().split(":", 1)
                    if len(key_value) == 2:
                        key = key_value[0].strip()
                        value = key_value[1].strip()
                        
                        # Normalize key names to match your usage
                        if key == "Source":
                            chunk_data["Source_File_Path"] = value
                        else:
                            chunk_data[key] = value
            
            # Get the actual content
            chunk_data["content"] = text.strip()
        else:
            chunk_data["content"] = content
        
        return chunk_data
    
    async def _check_existing_documents(self, chunk_ids: List[str]) -> set:
        """Check which chunk IDs already exist in the index."""
        existing_ids = set()
        if not chunk_ids:
            return existing_ids
        # Exact existence check per key; avoids the 1000-results ceiling
        for cid in chunk_ids:
            try:
                _ = self.search_client.get_document(key=cid)
                existing_ids.add(cid)
            except Exception:
                pass
        if existing_ids:
            log.info(f"📋 Found {len(existing_ids)} existing documents, will skip: {', '.join(list(existing_ids)[:5])}")
        return existing_ids
    
    def _choose_chunk_key(self, chunk_data: Dict, chunk_file: Path) -> str:
        """Choose a stable, de-duplicated chunk key."""
        # Prefer explicit header values if present
        explicit = chunk_data.get("Chunk ID") or chunk_data.get("chunk_id") or chunk_data.get("chunkKey")
        if explicit:
            return str(explicit).strip()
        # Fall back to a deterministic hash
        basis = "|".join([
            str(chunk_data.get("Source_File_Path", "")),
            str(chunk_data.get("Index", "")),
            str(chunk_data.get("Document", "")),
            chunk_file.name
        ])
        content_hash = hashlib.sha1(chunk_data.get("content", "").encode("utf-8")).hexdigest()[:12]
        return hashlib.sha1(f"{basis}|{content_hash}".encode("utf-8")).hexdigest()
    
    def _prepare_document_for_upload(self, chunk_data: Dict, chunk_id: str) -> Dict[str, Any]:
        """Prepare a document for upload to the vector database."""
        # Extract metadata
        doc_name = chunk_data.get("Document", "unknown")
        doc_type = chunk_data.get("Document Type", chunk_data.get("documentType", chunk_data.get("document_type", "unknown")))
        
        # Parse page info
        index_str = chunk_data.get("Index", "1/1")
        if "/" in index_str:
            current, total = index_str.split("/")
            start_page = int(current)
            end_page = int(current)  # For single chunks
        else:
            start_page = 1
            end_page = 1
        
        # Robust date parsing: accept MM.DD.YYYY, YYYY-MM-DD, MM/DD/YYYY, YYYY/MM/DD
        meeting_date_raw = chunk_data.get("Meeting Date", chunk_data.get("meetingDate", chunk_data.get("meeting_date", ""))) or ""
        meeting_date = None
        if meeting_date_raw:
            for fmt in ("%m.%d.%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(meeting_date_raw.strip(), fmt)
                    meeting_date = dt.strftime("%Y-%m-%dT00:00:00Z")
                    break
                except ValueError:
                    continue
        
        # Create document
        document = {
            "chunkKey": chunk_id,
            "sourceDocument": doc_name,
            "content": chunk_data.get("content", ""),
            "startPage": start_page,
            "endPage": end_page,
            "documentType": doc_type
        }
        
        # Only add Date field if we have a valid date
        if meeting_date:
            document["Date"] = meeting_date
        
        return document
    
    async def process_chunk_batch(self, chunk_files: List[Path]) -> int:
        """Process a batch of chunk files."""
        # First, collect all chunk IDs and check for existing documents
        chunk_ids = []
        chunk_file_map = {}
        
        for chunk_file in chunk_files:
            chunk_data = self._read_chunk_file(chunk_file)
            chunk_id = self._choose_chunk_key(chunk_data, chunk_file)
            chunk_ids.append(chunk_id)
            chunk_file_map[chunk_id] = chunk_file
        
        # Check which documents already exist
        existing_ids = await self._check_existing_documents(chunk_ids)
        
        documents = []
        skipped_count = 0
        
        for chunk_file in chunk_files:
            try:
                # Generate chunk ID (robust)
                chunk_data = self._read_chunk_file(chunk_file)
                chunk_id = self._choose_chunk_key(chunk_data, chunk_file)
                
                # Skip if document already exists
                if chunk_id in existing_ids:
                    skipped_count += 1
                    log.debug(f"⏭️ Skipping existing document: {chunk_id}")
                    continue
                
                # Prepare document
                doc = self._prepare_document_for_upload(chunk_data, chunk_id)
                
                # Generate embedding for content
                if doc["content"]:
                    embedding = await self.generate_embedding(doc["content"])
                    # Guard: ensure vector dims match index configuration
                    if len(embedding) != self.vector_dim:
                        raise ValueError(
                            f"Embedding dim {len(embedding)} != configured VECTOR_DIM {self.vector_dim}. "
                            f"Model '{self.embeddings_model}' must match your index."
                        )
                    doc["vector"] = embedding
                else:
                    log.warning(f"Skipping chunk {chunk_id} - no content")
                    continue
                
                documents.append(doc)
                
            except Exception as e:
                log.error(f"Failed to process chunk {chunk_file.name}: {e}")
                continue
        
        # Log skipping summary
        if skipped_count > 0:
            log.info(f"⏭️ Skipped {skipped_count} existing documents, processing {len(documents)} new documents")
        
        # Upload documents to search index
        if documents:
            try:
                result = self.search_client.upload_documents(documents=documents)
                succeeded = sum(1 for r in result if r.succeeded)
                log.info(f"✅ Uploaded {succeeded}/{len(documents)} documents")
                return succeeded
            except Exception as e:
                log.error(f"Failed to upload documents: {e}")
                return 0
        
        return 0
    
    async def push_all_chunks(self) -> int:
        """Push all document chunks to the vector database."""
        log.info("🚀 Starting vector database push")
        
        # Initialize index
        await self.initialize_index()
        
        # Get all chunk files
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        log.info(f"Found {len(chunk_files)} chunks to process")
        
        if not chunk_files:
            log.warning("No chunk files found")
            return 0
        
        # Process in batches
        total_uploaded = 0
        
        for i in range(0, len(chunk_files), self.batch_size):
            batch = chunk_files[i:i + self.batch_size]
            log.info(f"Processing batch {i//self.batch_size + 1}/{(len(chunk_files) + self.batch_size - 1)//self.batch_size}")
            
            uploaded = await self.process_chunk_batch(batch)
            total_uploaded += uploaded
            
            # Small delay between batches
            if i + self.batch_size < len(chunk_files):
                await asyncio.sleep(0.5)
        
        log.info(f"✅ Vector database push completed: {total_uploaded} documents uploaded")
        return total_uploaded
    
    async def verify_upload(self, sample_size: int = 5):
        """Verify that documents were uploaded correctly."""
        try:
            # Search for documents
            results = self.search_client.search(
                search_text="*",
                select=["chunkKey", "sourceDocument", "documentType"],
                top=sample_size
            )
            
            log.info(f"📋 Sample uploaded documents:")
            for result in results:
                log.info(f"  - {result['chunkKey']}: {result['sourceDocument']} ({result['documentType']})")
                
        except Exception as e:
            log.error(f"Failed to verify upload: {e}")


# Integration function
async def push_chunks_to_vector_db(chunks_dir: Path, output_dir: Optional[Path] = None) -> int:
    """
    Push document chunks to Azure Cognitive Search vector database.
    
    Args:
        chunks_dir: Directory containing document chunks
        output_dir: Optional output directory for logs/reports
        
    Returns:
        Number of documents uploaded
    """
    if not output_dir:
        output_dir = chunks_dir.parent
    
    pusher = VectorDatabasePusher(chunks_dir, output_dir)
    
    # Push all chunks
    uploaded_count = await pusher.push_all_chunks()
    
    # Verify upload
    await pusher.verify_upload()
    
    return uploaded_count