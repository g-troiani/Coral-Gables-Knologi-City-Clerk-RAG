#!/usr/bin/env python3
"""
Process 2019 Documents for NER Ontology Generation (Parallelized)

This script:
1. Traverses the 2019 folder inside city_clerk_documents/global/City Comissions 2024/
2. For each subfolder, creates a new API_[subfolder_name] directory
3. For each document in the original subfolders, calls Azure OpenAI with a prompt about NER entities/relationships
4. Saves the API response in markdown format with the original filename + "_API_response"
5. Uses async processing with rate limiting for efficient parallel processing
"""

import os
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from openai import AsyncAzureOpenAI
import time
from tqdm.asyncio import tqdm as async_tqdm

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

class AsyncDocument2019Processor:
    """Process 2019 documents and generate NER ontology suggestions with parallel processing."""
    
    def __init__(self, max_concurrent: int = 5, test_mode: bool = False, test_limit: int = 3):
        # Base path to the 2019 folder
        self.base_path = Path("city_clerk_documents/global/City Comissions 2024/2019")
        
        # Rate limiting configuration
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.call_times = []
        self.max_calls_per_minute = 50  # Conservative limit
        self.delay_between_calls = 1.2  # Seconds between calls
        
        # Test mode settings
        self.test_mode = test_mode
        self.test_limit = test_limit
        
        # Initialize Azure OpenAI client
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").split(" #")[0].strip().strip('"')
        self.client = AsyncAzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=endpoint
        )
        
        # Get Azure deployment name
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        if not self.model:
            raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")
        self.model = self.model.split('"')[0].strip()
        
        # Configuration
        self.max_tokens = 16384
        self.temperature = 0.0
        
        # NER ontology prompt
        self.ner_prompt = """What are all the types of entities and relationships/actions (NER) that would be useful to extract and push to a knowledge graph in the following piece of text? The kinds of entities and relationships/actions need to be included in an ontology.

Please analyze the text and provide:

1. **Entity Types** - All types of entities that should be extracted (e.g., Person, Organization, Location, Date, Document, etc.)
2. **Relationship Types** - All types of relationships/actions between entities (e.g., "approved", "voted", "proposed", "scheduled", etc.)
3. **Attributes** - Important properties that entities should have (e.g., document numbers, dates, addresses, etc.)

For each entity and relationship type, provide:
- A clear definition
- Examples from the text
- Why it would be valuable for a knowledge graph

Format your response as a structured analysis that could guide the creation of a comprehensive ontology for city government documents.

Text to analyze:
"""

    def validate_environment(self) -> bool:
        """Validate that all required environment variables are set."""
        required_vars = [
            "AZURE_OPENAI_API_KEY",
            "AZURE_OPENAI_ENDPOINT", 
            "AZURE_OPENAI_DEPLOYMENT_NAME"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            log.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
            log.error("Please set these in your .env file")
            return False
        
        log.info("✅ Environment variables validated")
        return True

    def get_subfolders(self) -> List[Path]:
        """Get all subfolders in the 2019 directory, excluding Resolution folders."""
        if not self.base_path.exists():
            log.error(f"❌ Base path does not exist: {self.base_path}")
            return []
        
        # Folders to exclude
        exclude_folders = {
            "Resolutions 2019",
            "API_Resolutions 2019", 
            "API_API_Resolutions 2019"
        }
        
        all_subfolders = [d for d in self.base_path.iterdir() if d.is_dir()]
        subfolders = [d for d in all_subfolders if d.name not in exclude_folders]
        
        excluded = [d.name for d in all_subfolders if d.name in exclude_folders]
        if excluded:
            log.info(f"📁 Excluding folders: {excluded}")
        
        log.info(f"📁 Found {len(subfolders)} subfolders to process: {[d.name for d in subfolders]}")
        return subfolders

    def create_api_folder(self, original_folder: Path) -> Path:
        """Create API_[subfolder_name] directory."""
        api_folder_name = f"API_{original_folder.name}"
        api_folder_path = original_folder.parent / api_folder_name
        
        api_folder_path.mkdir(exist_ok=True)
        log.info(f"📁 Created/verified API folder: {api_folder_path}")
        return api_folder_path

    def read_document_content(self, file_path: Path) -> str:
        """Read the content of a document file."""
        try:
            # Try to read as text first
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            return content
        except UnicodeDecodeError:
            # If that fails, try with different encoding
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    content = f.read()
                return content
            except Exception as e:
                log.warning(f"⚠️ Could not read {file_path} as text: {e}")
                # For binary files like PDFs, return a placeholder
                return f"[Binary file: {file_path.name}] - Content extraction would require additional processing"

    async def rate_limit(self):
        """Implement rate limiting to avoid hitting API limits."""
        now = time.time()
        
        # Remove old timestamps (older than 1 minute)
        self.call_times = [t for t in self.call_times if now - t < 60]
        
        # If we're approaching the rate limit, wait
        if len(self.call_times) >= self.max_calls_per_minute - 5:
            sleep_time = 60 - (now - self.call_times[0])
            if sleep_time > 0:
                log.info(f"⏳ Rate limit approaching, sleeping for {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
        
        # Add delay between calls
        await asyncio.sleep(self.delay_between_calls)
        
        # Record this call
        self.call_times.append(time.time())

    async def call_azure_openai_async(self, document_content: str, document_name: str) -> str:
        """Call Azure OpenAI API with the NER ontology prompt (async version)."""
        async with self.semaphore:
            await self.rate_limit()
            
            # Truncate content if too long (keeping within token limits)
            max_content_length = 10000  # Conservative limit to leave room for prompt and response
            if len(document_content) > max_content_length:
                document_content = document_content[:max_content_length] + "\n\n[Content truncated for analysis...]"
            
            full_prompt = self.ner_prompt + "\n\n" + document_content
            
            try:
                log.debug(f"🤖 Calling Azure OpenAI for document: {document_name}")
                
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an expert in knowledge graph ontology design and named entity recognition for government documents. Provide detailed, structured analysis."
                        },
                        {
                            "role": "user", 
                            "content": full_prompt
                        }
                    ],
                    temperature=self.temperature,
                    max_tokens=self.max_tokens
                )
                
                api_response = response.choices[0].message.content.strip()
                log.info(f"✅ Received response for {document_name} ({len(api_response)} characters)")
                return api_response
                
            except Exception as e:
                log.error(f"❌ API call failed for {document_name}: {e}")
                return f"Error generating ontology analysis: {str(e)}"

    async def save_api_response_async(self, api_folder: Path, original_filename: str, document_content: str, api_response: str) -> None:
        """Save the API response as a markdown file (async version)."""
        # Create output filename
        base_name = Path(original_filename).stem
        output_filename = f"{base_name}_API_response.md"
        output_path = api_folder / output_filename
        
        # Create markdown content
        markdown_content = f"""# NER Ontology Analysis for {original_filename}

## Document Information
- **Original File**: {original_filename}
- **Analysis Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}
- **Generated By**: Azure OpenAI NER Ontology Processor (Parallelized)

## Original Document Content Preview
```
{document_content[:1000]}{'...' if len(document_content) > 1000 else ''}
```

## NER Ontology Analysis

{api_response}

---
*This analysis was generated automatically to assist in knowledge graph ontology development.*
"""
        
        try:
            # Use asyncio to write file without blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._write_file, output_path, markdown_content)
            log.debug(f"💾 Saved API response to: {output_path}")
        except Exception as e:
            log.error(f"❌ Failed to save API response for {original_filename}: {e}")

    def _write_file(self, output_path: Path, content: str):
        """Helper method to write file synchronously."""
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)

    async def process_document(self, file_path: Path, api_folder: Path) -> bool:
        """Process a single document asynchronously."""
        try:
            # Read document content
            document_content = self.read_document_content(file_path)
            
            # Call Azure OpenAI API
            api_response = await self.call_azure_openai_async(document_content, file_path.name)
            
            # Save the response
            await self.save_api_response_async(api_folder, file_path.name, document_content, api_response)
            
            return True
        except Exception as e:
            log.error(f"❌ Failed to process {file_path.name}: {e}")
            return False

    async def process_folder_async(self, folder_path: Path) -> None:
        """Process all documents in a single folder asynchronously."""
        log.info(f"📂 Processing folder: {folder_path.name}")
        
        # Create the API folder
        api_folder = self.create_api_folder(folder_path)
        
        # Get all files in the folder
        files = [f for f in folder_path.iterdir() if f.is_file() and not f.name.startswith('.')]
        
        # In test mode, limit the number of files
        if self.test_mode:
            files = files[:self.test_limit]
            log.info(f"🧪 Test mode: Processing only {len(files)} files")
        else:
            log.info(f"📄 Found {len(files)} files to process")
        
        # Create tasks for all files
        tasks = [self.process_document(file_path, api_folder) for file_path in files]
        
        # Process all files with progress bar
        results = []
        async for result in async_tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Processing {folder_path.name}"):
            results.append(await result)
        
        successful = sum(results)
        log.info(f"✅ Completed folder {folder_path.name}: {successful}/{len(files)} files processed successfully")

    async def process_all_folders_async(self) -> None:
        """Process all subfolders in the 2019 directory asynchronously."""
        log.info("🚀 Starting 2019 document processing (parallelized)...")
        
        if self.test_mode:
            log.info(f"🧪 Running in TEST MODE - will process max {self.test_limit} files per folder")
        
        # Validate environment
        if not self.validate_environment():
            return
        
        # Get all subfolders
        subfolders = self.get_subfolders()
        if not subfolders:
            log.error("❌ No subfolders found to process")
            return
        
        log.info(f"⚡ Using {self.max_concurrent} concurrent API calls with rate limiting")
        
        # Process each subfolder
        for folder in subfolders:
            try:
                await self.process_folder_async(folder)
            except Exception as e:
                log.error(f"❌ Failed to process folder {folder.name}: {e}")
                continue
        
        # Clean up client
        await self.client.close()
        
        log.info("🎉 All folders processed successfully!")

async def main(test_mode: bool = False, test_limit: int = 3, max_concurrent: int = 5):
    """Main async entry point."""
    processor = AsyncDocument2019Processor(
        max_concurrent=max_concurrent, 
        test_mode=test_mode, 
        test_limit=test_limit
    )
    await processor.process_all_folders_async()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process 2019 documents for NER ontology generation")
    parser.add_argument("--test", action="store_true", help="Run in test mode (process only a few files)")
    parser.add_argument("--test-limit", type=int, default=3, help="Number of files to process per folder in test mode")
    parser.add_argument("--concurrent", type=int, default=5, help="Maximum concurrent API calls")
    
    args = parser.parse_args()
    
    # Run the async main function
    asyncio.run(main(
        test_mode=args.test,
        test_limit=args.test_limit,
        max_concurrent=args.concurrent
    )) 