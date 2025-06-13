"""
General utilities for the unified GraphRAG pipeline.
"""

import logging
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
import openai
from openai import AsyncOpenAI
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging(level: str = "INFO", log_file: Optional[Path] = None) -> None:
    """
    Setup logging configuration for the pipeline.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional file to write logs to
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


def ensure_directory_exists(directory: Path) -> None:
    """
    Ensure that a directory exists, creating it if necessary.
    
    Args:
        directory: Path to the directory
    """
    directory.mkdir(parents=True, exist_ok=True)


def get_llm_client(api_key: Optional[str] = None) -> AsyncOpenAI:
    """
    Get configured OpenAI client for LLM operations.
    
    Args:
        api_key: Optional API key, will use environment variable if not provided
        
    Returns:
        Configured AsyncOpenAI client
    """
    api_key = api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OpenAI API key is required")
    
    return AsyncOpenAI(api_key=api_key)


def clean_json_response(response_text: str) -> Dict[str, Any]:
    """
    Clean and parse JSON response from LLM, handling common formatting issues.
    
    Args:
        response_text: Raw response text from LLM
        
    Returns:
        Parsed JSON dictionary
    """
    # Remove markdown code blocks if present
    if "```json" in response_text:
        response_text = response_text.split("```json")[1].split("```")[0]
    elif "```" in response_text:
        response_text = response_text.split("```")[1].split("```")[0]
    
    # Remove any leading/trailing whitespace
    response_text = response_text.strip()
    
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        # Try to fix common JSON issues
        fixed_text = response_text.replace("'", '"')  # Single to double quotes
        fixed_text = re.sub(r',\s*}', '}', fixed_text)  # Remove trailing commas
        fixed_text = re.sub(r',\s*]', ']', fixed_text)  # Remove trailing commas in arrays
        
        try:
            return json.loads(fixed_text)
        except json.JSONDecodeError:
            logging.error(f"Failed to parse JSON response: {response_text[:200]}...")
            raise e


def extract_metadata_from_header(content: str) -> Dict[str, Any]:
    """
    Extract metadata from markdown header section.
    
    Args:
        content: Markdown content with metadata header
        
    Returns:
        Dictionary of extracted metadata
    """
    metadata = {}
    
    # Look for metadata section between --- markers
    if content.startswith("---"):
        try:
            _, header_section, _ = content.split("---", 2)
            for line in header_section.strip().split("\n"):
                if ":" in line:
                    key, value = line.split(":", 1)
                    metadata[key.strip()] = value.strip()
        except ValueError:
            pass  # No proper YAML header found
    
    # Also look for simple key-value pairs at the beginning
    lines = content.split("\n")
    for line in lines[:20]:  # Check first 20 lines
        if line.startswith("**") and ":**" in line:
            # Extract from bold formatting like **Title:** Something
            key_match = re.search(r'\*\*(.*?)\*\*:\s*(.*)', line)
            if key_match:
                key = key_match.group(1).lower().replace(" ", "_")
                value = key_match.group(2)
                metadata[key] = value
    
    return metadata


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename by removing or replacing invalid characters.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename safe for filesystem
    """
    # Replace problematic characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove multiple consecutive underscores
    filename = re.sub(r'_{2,}', '_', filename)
    # Remove leading/trailing underscores and dots
    filename = filename.strip("_.")
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename


def chunk_text(text: str, chunk_size: int = 4000, overlap: int = 200) -> List[str]:
    """
    Split text into overlapping chunks for processing.
    
    Args:
        text: Text to chunk
        chunk_size: Maximum size of each chunk
        overlap: Number of characters to overlap between chunks
        
    Returns:
        List of text chunks
    """
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        
        # Try to break at a sentence or paragraph boundary
        if end < len(text):
            # Look for sentence endings within the last 200 characters
            search_start = max(end - 200, start)
            sentence_end = text.rfind(".", search_start, end)
            if sentence_end > start:
                end = sentence_end + 1
            else:
                # Look for paragraph breaks
                para_end = text.rfind("\n\n", search_start, end)
                if para_end > start:
                    end = para_end + 2
        
        chunks.append(text[start:end])
        
        if end >= len(text):
            break
            
        start = end - overlap
    
    return chunks


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


async def call_llm_with_retry(
    client: AsyncOpenAI,
    messages: List[Dict[str, str]],
    model: str = "gpt-4",
    max_retries: int = 3,
    **kwargs
) -> str:
    """
    Call LLM with retry logic for handling rate limits and transient errors.
    
    Args:
        client: OpenAI client
        messages: List of message dictionaries
        model: Model to use
        max_retries: Maximum number of retries
        **kwargs: Additional arguments for the API call
        
    Returns:
        Response text from the LLM
    """
    import asyncio
    
    for attempt in range(max_retries + 1):
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                **kwargs
            )
            return response.choices[0].message.content
        except Exception as e:
            if attempt == max_retries:
                raise e
            
            # Wait before retrying (exponential backoff)
            wait_time = 2 ** attempt
            logging.warning(f"LLM call failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
            await asyncio.sleep(wait_time) 