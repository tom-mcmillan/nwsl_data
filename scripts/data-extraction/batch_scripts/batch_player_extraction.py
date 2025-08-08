#!/usr/bin/env python3
"""
Batch Player Extraction - Process all HTML files and populate match_player table
Following the scraping.md methodology for production-scale extraction
"""

import logging
import os
import sys
import time
from datetime import datetime

# Add current directory to path to import our modules
sys.path.append("/Users/thomasmcmillan/projects/nwsl_data")

from database_inserter import DatabaseInserter
from fbref_player_extractor import FBRefPlayerExtractor

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/Users/thomasmcmillan/projects/nwsl_data/player_extraction.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class BatchPlayerExtraction:
    """
    Batch process all HTML files to extract player stats and populate database
    Following production practices from scraping.md
    """

    def __init__(self, html_dir: str, db_path: str):
        self.html_dir = html_dir
        self.db_path = db_path
        self.extractor = FBRefPlayerExtractor(db_path)
        self.inserter = DatabaseInserter(db_path)

        # Tracking
        self.total_files = 0
        self.processed_files = 0
        self.failed_files = 0
        self.total_players = 0
        self.start_time = None

    def get_html_files(self) -> list[str]:
        """Get all HTML match files to process"""
        html_files = [f for f in os.listdir(self.html_dir) if f.endswith(".html") and f.startswith("match_")]
        return sorted(html_files)

    def check_existing_data(self) -> dict[str, int]:
        """Check which matches already have player data"""
        import sqlite3

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get matches that already have player data
        cursor.execute("""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            GROUP BY match_id
        """)
        existing_data = {row[0]: row[1] for row in cursor.fetchall()}

        # Get total matches in database
        cursor.execute("SELECT COUNT(*) FROM match")
        total_matches = cursor.fetchone()[0]

        conn.close()

        logger.info("📊 Database status:")
        logger.info(f"  Total matches in database: {total_matches}")
        logger.info(f"  Matches with player data: {len(existing_data)}")
        logger.info(f"  Total player records: {sum(existing_data.values())}")

        return existing_data

    def process_all_files(self, skip_existing: bool = True) -> dict[str, any]:
        """
        Process all HTML files and extract player data
        Following scraping.md production methodology
        """

        self.start_time = time.time()

        # Get all HTML files
        html_files = self.get_html_files()
        self.total_files = len(html_files)

        logger.info("🚀 Starting batch player extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total HTML files: {self.total_files}")

        # Check existing data
        existing_data = self.check_existing_data()

        # Filter files if skipping existing
        files_to_process = []
        for html_file in html_files:
            match_id = html_file.replace("match_", "").replace(".html", "")
            if skip_existing and match_id in existing_data:
                logger.debug(f"⏭️  Skipping {match_id} - already has {existing_data[match_id]} player records")
                continue
            files_to_process.append(html_file)

        logger.info(f"📝 Files to process: {len(files_to_process)}")
        logger.info(f"⏭️  Files to skip: {self.total_files - len(files_to_process)}")

        if not files_to_process:
            logger.info("✅ All files already processed!")
            return self._get_summary()

        # Process each file
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        for i, html_file in enumerate(files_to_process, 1):
            self._process_single_file(html_file, i, len(files_to_process))

        # Final summary
        return self._print_final_summary()

    def _process_single_file(self, html_file: str, current: int, total: int):
        """Process a single HTML file"""

        html_path = os.path.join(self.html_dir, html_file)
        match_id = html_file.replace("match_", "").replace(".html", "")

        logger.info(f"[{current}/{total}] Processing {match_id}...")

        try:
            # Extract player stats using FBRef methodology
            success = self.extractor.process_html_file(html_path)

            if success and self.extractor.processed_matches:
                # Get the extracted data
                _, players_data = self.extractor.processed_matches[-1]

                # Insert into database
                if self.inserter.insert_match_players(players_data):
                    self.processed_files += 1
                    self.total_players += len(players_data)
                    logger.info(f"✅ Success! Extracted {len(players_data)} players")
                else:
                    self.failed_files += 1
                    logger.error(f"❌ Database insertion failed for {match_id}")
            else:
                self.failed_files += 1
                logger.error(f"❌ Extraction failed for {match_id}")

        except Exception as e:
            self.failed_files += 1
            logger.error(f"❌ Error processing {match_id}: {str(e)}")

        # Progress update every 50 files
        if current % 50 == 0:
            elapsed = time.time() - self.start_time
            rate = current / elapsed * 60  # files per minute
            remaining = (total - current) / rate if rate > 0 else 0

            logger.info(f"📊 Progress: {current}/{total} ({current/total*100:.1f}%)")
            logger.info(f"⏱️  Rate: {rate:.1f} files/min, Est. remaining: {remaining:.1f} min")
            logger.info(f"✅ Success: {self.processed_files}, ❌ Failed: {self.failed_files}")

    def _print_final_summary(self) -> dict[str, any]:
        """Print final extraction summary"""

        elapsed = time.time() - self.start_time

        logger.info(f"\n{'='*60}")
        logger.info(f"EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Files processed: {self.processed_files}/{self.total_files}")
        logger.info(f"✅ Success rate: {self.processed_files/(self.processed_files+self.failed_files)*100:.1f}%")
        logger.info(f"👥 Total players extracted: {self.total_players}")
        logger.info(f"📊 Average players per match: {self.total_players/self.processed_files:.1f}")
        logger.info(f"⚡ Processing rate: {self.processed_files/elapsed*60:.1f} files/min")

        if self.failed_files > 0:
            logger.info(f"\n❌ Failed files: {self.failed_files}")
            logger.info("Check logs for specific error details")

        logger.info(f"{'='*60}")

        return self._get_summary()

    def _get_summary(self) -> dict[str, any]:
        """Get extraction summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0

        return {
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "failed_files": self.failed_files,
            "total_players": self.total_players,
            "elapsed_time": elapsed,
            "success_rate": self.processed_files / (self.processed_files + self.failed_files) * 100
            if (self.processed_files + self.failed_files) > 0
            else 0,
            "processing_rate": self.processed_files / elapsed * 60 if elapsed > 0 else 0,
        }


def run_full_extraction():
    """Run the complete extraction process"""

    html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    batch_processor = BatchPlayerExtraction(html_dir, db_path)
    results = batch_processor.process_all_files(skip_existing=True)

    return results


def run_test_extraction(num_files: int = 5):
    """Run a test extraction on limited files"""

    html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    logger.info(f"🧪 Running TEST extraction on {num_files} files")

    batch_processor = BatchPlayerExtraction(html_dir, db_path)

    # Get limited file list
    all_files = batch_processor.get_html_files()
    test_files = all_files[:num_files]

    # Temporarily override total files for test
    batch_processor.total_files = len(test_files)
    batch_processor.start_time = time.time()

    logger.info(f"🧪 Testing with files: {test_files}")

    for i, html_file in enumerate(test_files, 1):
        batch_processor._process_single_file(html_file, i, len(test_files))

    return batch_processor._print_final_summary()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Run test mode
        run_test_extraction(5)
    else:
        # Run full extraction
        run_full_extraction()
