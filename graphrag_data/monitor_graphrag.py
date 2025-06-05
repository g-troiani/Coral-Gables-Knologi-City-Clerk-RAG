#!/usr/bin/env python3
"""
GraphRAG Monitoring and Logging Script
Monitors GraphRAG indexing process with detailed logging and status updates
"""

import os
import sys
import time
import subprocess
import signal
from datetime import datetime
from pathlib import Path
import logging

class GraphRAGMonitor:
    def __init__(self, base_dir="."):
        self.base_dir = Path(base_dir)
        self.log_dir = self.base_dir / "logs"
        self.output_dir = self.base_dir / "output"
        self.log_dir.mkdir(exist_ok=True)
        
        # Set up comprehensive logging
        self.setup_logging()
        self.process = None
        self.start_time = None
        
    def setup_logging(self):
        """Set up detailed logging with multiple handlers"""
        self.logger = logging.getLogger("GraphRAGMonitor")
        self.logger.setLevel(logging.DEBUG)
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Console handler for real-time feedback
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        # File handler for detailed logs
        log_file = self.log_dir / f"graphrag_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)
        
        self.log_file_path = log_file
        self.logger.info(f"📋 Monitoring logs will be saved to: {log_file}")
        
    def check_prerequisites(self):
        """Check if all prerequisites are met"""
        self.logger.info("🔍 Checking prerequisites...")
        
        # Check API key
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            self.logger.error("❌ OPENAI_API_KEY environment variable not set")
            return False
        elif api_key.startswith('sk-') and len(api_key) > 20:
            self.logger.info("✅ OpenAI API key found and appears valid")
        else:
            self.logger.warning("⚠️  API key format seems unusual")
        
        # Check input file
        csv_file = self.base_dir / "city_clerk_documents.csv"
        if not csv_file.exists():
            self.logger.error(f"❌ Input file not found: {csv_file}")
            return False
        
        file_size = csv_file.stat().st_size
        self.logger.info(f"✅ Input file found: {csv_file} ({file_size:,} bytes)")
        
        # Check settings
        settings_file = self.base_dir / "settings.yaml"
        if not settings_file.exists():
            self.logger.error(f"❌ Settings file not found: {settings_file}")
            return False
        self.logger.info(f"✅ Settings file found: {settings_file}")
        
        return True
        
    def monitor_output_files(self):
        """Monitor and report on output files being created"""
        expected_files = [
            "entities.parquet",
            "relationships.parquet", 
            "communities.parquet",
            "community_reports.parquet",
            "text_units.parquet",
            "create_final_nodes.parquet",
            "create_final_edges.parquet"
        ]
        
        if not self.output_dir.exists():
            return {}
            
        found_files = {}
        for file_name in expected_files:
            file_path = self.output_dir / file_name
            if file_path.exists():
                size = file_path.stat().st_size
                found_files[file_name] = size
                
        return found_files
        
    def monitor_progress(self):
        """Monitor the GraphRAG process progress"""
        self.logger.info("📊 Starting progress monitoring...")
        last_files = {}
        
        while self.process and self.process.poll() is None:
            # Check output files
            current_files = self.monitor_output_files()
            
            # Report new files or size changes
            for file_name, size in current_files.items():
                if file_name not in last_files:
                    self.logger.info(f"📄 New output file created: {file_name} ({size:,} bytes)")
                elif size != last_files[file_name]:
                    self.logger.info(f"📈 File updated: {file_name} ({size:,} bytes)")
            
            last_files = current_files.copy()
            
            # Show elapsed time
            if self.start_time:
                elapsed = datetime.now() - self.start_time
                self.logger.debug(f"⏱️  Elapsed time: {elapsed}")
            
            time.sleep(30)  # Check every 30 seconds
            
    def run_graphrag(self):
        """Run GraphRAG indexing with monitoring"""
        if not self.check_prerequisites():
            self.logger.error("❌ Prerequisites check failed. Cannot start GraphRAG.")
            return False
            
        self.logger.info("🚀 Starting GraphRAG indexing process...")
        self.start_time = datetime.now()
        
        # Prepare command
        venv_python = Path("../venv/bin/python3")
        if not venv_python.exists():
            # Try alternative path
            venv_python = Path("../venv/bin/python")
            
        cmd = ["../venv/bin/python3", "-m", "graphrag", "index", "--verbose"]
        
        # Set up environment
        env = os.environ.copy()
        
        try:
            # Start the process with detailed logging
            self.logger.info(f"🔧 Command: {' '.join(cmd)}")
            self.logger.info(f"📂 Working directory: {self.base_dir}")
            
            self.process = subprocess.Popen(
                cmd,
                cwd=self.base_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                env=env
            )
            
            # Monitor in real-time
            self.logger.info("📡 Monitoring GraphRAG output...")
            
            # Read output line by line
            while True:
                output = self.process.stdout.readline()
                if output == '' and self.process.poll() is not None:
                    break
                if output:
                    # Log GraphRAG output
                    clean_output = output.strip()
                    if clean_output:
                        self.logger.info(f"GraphRAG: {clean_output}")
            
            # Wait for completion
            return_code = self.process.wait()
            end_time = datetime.now()
            total_time = end_time - self.start_time
            
            if return_code == 0:
                self.logger.info(f"✅ GraphRAG indexing completed successfully!")
                self.logger.info(f"⏱️  Total time: {total_time}")
                self.report_final_results()
                return True
            else:
                self.logger.error(f"❌ GraphRAG indexing failed with exit code: {return_code}")
                self.logger.error(f"⏱️  Failed after: {total_time}")
                return False
                
        except KeyboardInterrupt:
            self.logger.warning("⚠️  Process interrupted by user")
            if self.process:
                self.process.terminate()
            return False
        except Exception as e:
            self.logger.error(f"❌ Error running GraphRAG: {str(e)}")
            return False
            
    def report_final_results(self):
        """Report final results and file statistics"""
        self.logger.info("📊 Final Results Summary:")
        self.logger.info("=" * 50)
        
        output_files = self.monitor_output_files()
        if output_files:
            self.logger.info("📄 Output files created:")
            total_size = 0
            for file_name, size in output_files.items():
                self.logger.info(f"   - {file_name}: {size:,} bytes")
                total_size += size
            self.logger.info(f"📦 Total output size: {total_size:,} bytes")
        else:
            self.logger.warning("⚠️  No output files found")
            
        # Check for any error logs
        indexing_log = self.log_dir / "indexing-engine.log"
        if indexing_log.exists():
            self.logger.info(f"📋 GraphRAG logs available at: {indexing_log}")
            
        self.logger.info(f"📋 Monitor logs saved to: {self.log_file_path}")
        self.logger.info("=" * 50)

def main():
    """Main function"""
    print("🔍 GraphRAG Monitor Starting...")
    monitor = GraphRAGMonitor()
    
    def signal_handler(sig, frame):
        monitor.logger.warning("🛑 Received interrupt signal, shutting down...")
        if monitor.process:
            monitor.process.terminate()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    success = monitor.run_graphrag()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 