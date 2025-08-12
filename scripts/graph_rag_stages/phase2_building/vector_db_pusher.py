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
import openai
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
        self.index_name = os.getenv("VECTOR_DATABASE_NAME", "city-clerk-rag").strip()
        
        if not self.search_endpoint or not self.search_key:
            raise ValueError("Azure Search endpoint and key must be set in environment variables")
        
        # Initialize Azure OpenAI for embeddings
        self.openai_client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        # Embeddings model deployment name (you'll need to set this)
        self.embeddings_model = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT", "text-embedding-ada-002")
        
        # Make vector dims configurable (prevents silent 400s when model changes)
        self.vector_dim = int(os.getenv("VECTOR_DIM", "1536"))
        
        # Initialize search clients
        self.index_client = SearchIndexClient(
            endpoint=self.search_endpoint,
            credential=AzureKeyCredential(self.search_key)
        )
        
        self.search_client = SearchClient(
            endpoint=self.search_endpoint,
            index_name=self.index_name,
            credential=AzureKeyCredential(self.search_key)
        )
        
        # Batch settings
        self.batch_size = 100
        self.max_concurrent = 5
        
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
            
        try:
            # Get all existing documents and check which chunk IDs exist
            # This is more robust than filtering, especially if index schema changes
            results = self.search_client.search(
                search_text="*",
                select=["chunkKey"],
                top=1000  # Should be enough for most use cases
            )
            
            # Create set of existing chunk IDs
            all_existing_ids = set()
            for result in results:
                all_existing_ids.add(result["chunkKey"])
            
            # Find intersection with our batch
            existing_ids = set(chunk_ids) & all_existing_ids
                
            if existing_ids:
                log.info(f"📋 Found {len(existing_ids)} existing documents, will skip: {', '.join(list(existing_ids)[:5])}")
                
        except Exception as e:
            log.warning(f"Could not check for existing documents: {e}")
            # If we can't check, proceed anyway (better to have duplicates than miss documents)
            
        return existing_ids
    
    def _prepare_document_for_upload(self, chunk_data: Dict, chunk_id: str) -> Dict[str, Any]:
        """Prepare a document for upload to the vector database."""
        # Extract metadata
        doc_name = chunk_data.get("Document", "unknown")
        doc_type = chunk_data.get("Document Type", chunk_data.get("document_type", "unknown"))
        
        # Parse page info
        index_str = chunk_data.get("Index", "1/1")
        if "/" in index_str:
            current, total = index_str.split("/")
            start_page = int(current)
            end_page = int(current)  # For single chunks
        else:
            start_page = 1
            end_page = 1
        
        # Parse date
        meeting_date = chunk_data.get("Meeting Date", chunk_data.get("meeting_date", ""))
        if meeting_date:
            # Convert date format if needed
            try:
                if "." in meeting_date:
                    # Convert MM.DD.YYYY to ISO format
                    parts = meeting_date.split(".")
                    if len(parts) == 3:
                        month, day, year = parts
                        meeting_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}T00:00:00Z"
                    else:
                        meeting_date = None
                else:
                    meeting_date = None
            except:
                meeting_date = None
        else:
            meeting_date = None
        
        # Create document
        document = {
            "chunkKey": chunk_id,
            "sourceDocument": doc_name,
            "content": chunk_data.get("content", ""),
            "startPage": start_page,
            "endPage": end_page,   # match index schema
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
            chunk_id = chunk_file.stem.split("_")[0]
            chunk_ids.append(chunk_id)
            chunk_file_map[chunk_id] = chunk_file
        
        # Check which documents already exist
        existing_ids = await self._check_existing_documents(chunk_ids)
        
        documents = []
        skipped_count = 0
        
        for chunk_file in chunk_files:
            try:
                # Generate chunk ID
                chunk_id = chunk_file.stem.split("_")[0]
                
                # Skip if document already exists
                if chunk_id in existing_ids:
                    skipped_count += 1
                    log.debug(f"⏭️ Skipping existing document: {chunk_id}")
                    continue
                
                # Read chunk data
                chunk_data = self._read_chunk_file(chunk_file)
                
                # Prepare document
                doc = self._prepare_document_for_upload(chunk_data, chunk_id)
                
                # Generate embedding for content
                if doc["content"]:
                    embedding = await self.generate_embedding(doc["content"])
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