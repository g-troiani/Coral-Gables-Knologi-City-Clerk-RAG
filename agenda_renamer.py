#!/usr/bin/env python3
"""
Agenda Document Renamer using Azure OpenAI GPT-4
Extracts meeting dates from agenda PDFs and renames them to format: Agenda MM.DD.YYYY.pdf
"""

import os
import re
from pathlib import Path
from typing import Optional, List
import PyPDF2
from openai import AzureOpenAI
from dotenv import load_dotenv
import time
from datetime import datetime

# Load environment variables
load_dotenv()

class AgendaRenamer:
    def __init__(self):
        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        # Directory containing agenda PDFs
        self.agenda_dir = Path("city_clerk_documents/global/City Comissions 2024/Agendas")
        
        # Track processed files
        self.processed_files = []
        self.failed_files = []
        
    def extract_text_from_pdf(self, pdf_path: Path, max_pages: int = 2) -> str:
        """Extract text from the first few pages of a PDF."""
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                text = ""
                
                # Extract text from first max_pages pages
                for page_num in range(min(len(reader.pages), max_pages)):
                    page = reader.pages[page_num]
                    text += page.extract_text() + "\n"
                
                return text.strip()
        except Exception as e:
            print(f"❌ Error extracting text from {pdf_path}: {e}")
            return ""
    
    def extract_date_with_gpt4(self, text: str, filename: str) -> Optional[str]:
        """Use Azure OpenAI GPT-4 to extract meeting date from document text."""
        try:
            prompt = f"""
            You are analyzing a city council meeting agenda document. Extract the meeting date from this text.
            
            Document filename: {filename}
            
            Document text (first 1-2 pages):
            {text[:4000]}  # Limit text to avoid token limits
            
            Please identify the meeting date and return it in the format MM.DD.YYYY.
            
            Look for phrases like:
            - "Meeting Date:"
            - "Date:"
            - "Council Meeting"
            - Date patterns in headers or footers
            
            If you find multiple dates, return the main meeting date (not document preparation dates).
            
            Return ONLY the date in MM.DD.YYYY format. If no clear meeting date is found, return "UNKNOWN".
            """
            
            response = self.client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4.1-nano"),  # Using GPT-4.1 nano
                messages=[
                    {"role": "system", "content": "You are a precise document analyzer that extracts meeting dates from city council agendas."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=100,
                temperature=0.1
            )
            
            date_str = response.choices[0].message.content.strip()
            
            # Validate date format
            if re.match(r'\d{2}\.\d{2}\.\d{4}', date_str):
                return date_str
            else:
                print(f"⚠️  GPT-4 returned invalid date format: {date_str}")
                return None
                
        except Exception as e:
            print(f"❌ Error calling GPT-4 for {filename}: {e}")
            return None
    
    def rename_file(self, old_path: Path, new_date: str) -> bool:
        """Rename file to new format."""
        try:
            new_filename = f"Agenda {new_date}.pdf"
            new_path = old_path.parent / new_filename
            
            # Check if target file already exists
            if new_path.exists():
                print(f"⚠️  Target file already exists: {new_filename}")
                # Add a counter to make it unique
                counter = 1
                while new_path.exists():
                    new_filename = f"Agenda {new_date}_{counter}.pdf"
                    new_path = old_path.parent / new_filename
                    counter += 1
            
            # Rename the file
            old_path.rename(new_path)
            print(f"✅ Renamed: {old_path.name} → {new_filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error renaming {old_path.name}: {e}")
            return False
    
    def process_single_file(self, pdf_path: Path) -> bool:
        """Process a single PDF file."""
        print(f"\n📄 Processing: {pdf_path.name}")
        
        # Skip if already in correct format
        if re.match(r'Agenda \d{2}\.\d{2}\.\d{4}.*\.pdf', pdf_path.name):
            print(f"✅ Already in correct format: {pdf_path.name}")
            return True
        
        # Extract text from PDF
        text = self.extract_text_from_pdf(pdf_path)
        if not text:
            print(f"❌ Could not extract text from {pdf_path.name}")
            return False
        
        # Extract date using GPT-4
        date_str = self.extract_date_with_gpt4(text, pdf_path.name)
        if not date_str:
            print(f"❌ Could not extract date from {pdf_path.name}")
            return False
        
        # Rename file
        success = self.rename_file(pdf_path, date_str)
        
        # Add small delay to avoid rate limiting
        time.sleep(0.5)
        
        return success
    
    def process_all_files(self):
        """Process all agenda PDF files in the directory."""
        if not self.agenda_dir.exists():
            print(f"❌ Directory not found: {self.agenda_dir}")
            return
        
        # Get all PDF files
        pdf_files = list(self.agenda_dir.glob("*.pdf"))
        print(f"🔍 Found {len(pdf_files)} PDF files to process")
        
        if not pdf_files:
            print("❌ No PDF files found in the directory")
            return
        
        # Process each file
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"\n{'='*60}")
            print(f"Processing file {i}/{len(pdf_files)}")
            
            try:
                if self.process_single_file(pdf_file):
                    self.processed_files.append(pdf_file.name)
                else:
                    self.failed_files.append(pdf_file.name)
            except Exception as e:
                print(f"❌ Unexpected error processing {pdf_file.name}: {e}")
                self.failed_files.append(pdf_file.name)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print processing summary."""
        print(f"\n{'='*60}")
        print("📊 PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(f"✅ Successfully processed: {len(self.processed_files)}")
        print(f"❌ Failed to process: {len(self.failed_files)}")
        
        if self.failed_files:
            print(f"\n❌ Failed files:")
            for file in self.failed_files:
                print(f"  - {file}")
        
        print(f"\n🎉 Processing complete!")

def main():
    """Main function to run the agenda renamer."""
    print("🚀 Starting Agenda Document Renamer")
    print("=" * 60)
    
    # Check required environment variables
    required_vars = ["AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_VERSION"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set them in your .env file:")
        for var in missing_vars:
            print(f"  {var}=your_value_here")
        return
    
    # Create and run renamer
    renamer = AgendaRenamer()
    renamer.process_all_files()

if __name__ == "__main__":
    main() 