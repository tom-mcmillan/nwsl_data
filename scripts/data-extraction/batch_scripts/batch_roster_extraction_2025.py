#!/usr/bin/env python3
"""
Batch roster extraction for 91 specified 2025 match IDs
Using the FIXED methodology that successfully extracted match 7239a666
"""

import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/Users/thomasmcmillan/projects/nwsl_data/roster_extraction_2025.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# List of 91 match IDs to process
MATCH_IDS = [
    "7239a666",
    "5f6913d7",
    "4158fb00",
    "f61689bf",
    "0194ccbe",
    "008e301f",
    "45ad1c98",
    "7958f078",
    "a4bd2b1e",
    "26889552",
    "2f0f155c",
    "b7b3a204",
    "0a35cfd4",
    "a19ff7be",
    "0877e051",
    "74349bed",
    "96890718",
    "a38568cb",
    "9ae009b7",
    "475a847a",
    "9e867b88",
    "c8df78e2",
    "dd55cdd7",
    "d0b68db4",
    "fb58cf7f",
    "3d114ae2",
    "031916dc",
    "733e8be0",
    "ecbd2ae5",
    "ba2ade47",
    "d7e4972c",
    "71e1c7c8",
    "0fbbc32f",
    "12b89923",
    "f09b4cef",
    "414d2972",
    "592098e0",
    "7eff59ff",
    "903440ae",
    "64bc17e9",
    "20423258",
    "1c087799",
    "dd365af8",
    "13e27d90",
    "8fc9e8b7",
    "0cf23a87",
    "c0cb6fdc",
    "f513c5f3",
    "56095b7f",
    "d19e8e40",
    "3329a1cc",
    "03442e78",
    "07c68416",
    "2f764197",
    "92952d3a",
    "6a832d3c",
    "383bbe06",
    "70ffc3bc",
    "6c8777ca",
    "edb68a59",
    "d2545219",
    "b2aea0ed",
    "42890885",
    "5d73d536",
    "d174fd4f",
    "eb55dce7",
    "79cbcac5",
    "b6095527",
    "30adae5d",
    "11e533b1",
    "39bffb5e",
    "4013be4a",
    "810cc4f5",
    "726a91eb",
    "60b53e0c",
    "60a0c5a1",
    "230c5ecb",
    "fc4833e0",
    "30d6a0e0",
    "2e27ac56",
    "ac197aa2",
    "11ec0554",
    "692f1ab8",
    "f06ed457",
    "612a6c4c",
    "91373610",
    "6be469ca",
    "14bc0f9a",
    "548d019f",
    "d40c64aa",
    "c56678cf",
]


class BatchRosterExtractor:
    """Batch extract roster data for 2025 matches using FIXED methodology"""

    def __init__(self):
        self.html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
        self.db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

        # Tracking
        self.processed_matches = 0
        self.failed_matches = []
        self.successful_matches = []
        self.total_players_extracted = 0
        self.start_time = None

    def process_all_matches(self):
        """Process all 91 match IDs"""

        self.start_time = time.time()

        logger.info("🚀 Starting batch roster extraction for 2025 season")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(MATCH_IDS)}")

        # Check which matches already have data
        existing_matches = self.check_existing_data()

        matches_to_process = []
        for match_id in MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)

        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(MATCH_IDS) - len(matches_to_process)}")

        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()

        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING BATCH EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))

        return self.generate_final_summary()

    def check_existing_data(self):
        """Check which matches already have roster data"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get matches that already have roster data
        placeholders = ",".join(["?" for _ in MATCH_IDS])
        cursor.execute(
            f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """,
            MATCH_IDS,
        )

        existing_data = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        logger.info("📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(MATCH_IDS)}")

        return existing_data

    def process_single_match(self, match_id, current, total):
        """Process a single match using FIXED methodology"""

        html_path = os.path.join(self.html_dir, f"match_{match_id}.html")

        logger.info(f"[{current}/{total}] Processing {match_id}...")

        try:
            # Check if HTML file exists
            if not os.path.exists(html_path):
                logger.error(f"❌ HTML file not found: {html_path}")
                self.failed_matches.append((match_id, "HTML file not found"))
                return

            # Extract roster data using FIXED methodology
            players = self.extract_roster_fixed(html_path, match_id)

            if players:
                # Insert into database
                if self.insert_roster_data(players):
                    self.successful_matches.append(match_id)
                    self.total_players_extracted += len(players)
                    logger.info(f"✅ Success! Extracted {len(players)} players")
                else:
                    self.failed_matches.append((match_id, "Database insertion failed"))
            else:
                self.failed_matches.append((match_id, "No players extracted"))

        except Exception as e:
            logger.error(f"❌ Error processing {match_id}: {str(e)}")
            self.failed_matches.append((match_id, str(e)))

        self.processed_matches += 1

        # Progress update every 10 matches
        if current % 10 == 0:
            elapsed = time.time() - self.start_time
            rate = current / elapsed * 60  # matches per minute
            remaining = (total - current) / rate if rate > 0 else 0

            logger.info(f"📊 Progress: {current}/{total} ({current/total*100:.1f}%)")
            logger.info(f"⏱️  Rate: {rate:.1f} matches/min, Est. remaining: {remaining:.1f} min")
            logger.info(f"✅ Success: {len(self.successful_matches)}, ❌ Failed: {len(self.failed_matches)}")

    def extract_roster_fixed(self, html_path, match_id):
        """Extract roster using FIXED methodology from successful 7239a666 extraction"""

        # Read HTML
        with open(html_path, encoding="utf-8") as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, "html.parser")

        # Find all tables to identify summary tables
        tables = soup.find_all("table")
        table_ids = [table.get("id") for table in tables if table.get("id")]

        # Find summary tables
        summary_tables = [tid for tid in table_ids if "stats_" in tid and "_summary" in tid]

        if not summary_tables:
            logger.warning(f"⚠️  No summary tables found for {match_id}")
            return []

        all_players = []

        # Process each team's summary table
        for table_id in summary_tables:
            team_id = table_id.split("_")[1]
            table = soup.find("table", id=table_id)

            if not table:
                continue

            # Convert to DataFrame
            df = pd.read_html(str(table))[0]

            # Handle MultiIndex columns with FIXED mapping
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns.values]

            # Process each player row
            for _idx, row in df.iterrows():
                player_name = str(row.iloc[0]).strip()

                # Filter out invalid rows and team totals
                if (
                    pd.isna(player_name)
                    or player_name == ""
                    or player_name.lower() in ["player", "nan"]
                    or "Players" in player_name
                ):
                    continue

                # Extract using FIXED column mapping
                player_data = {
                    "match_id": match_id,
                    "player_name": player_name,
                    "team_id": team_id,
                    "shirt_number": self.safe_extract_int(row, ["Unnamed: 1_level_0_#"]),
                    "minutes_played": self.safe_extract_int(row, ["Unnamed: 5_level_0_Min"]),
                }

                all_players.append(player_data)

        return all_players

    def safe_extract_int(self, row, possible_columns):
        """Safely extract integer value from row"""
        for col in possible_columns:
            if col in row.index:
                value = row[col]
                if pd.notna(value) and str(value).strip() != "":
                    try:
                        return int(float(str(value).replace(",", "")))
                    except:
                        continue
        return None

    def insert_roster_data(self, players_data):
        """Insert roster data into match_player table"""

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            inserted_count = 0

            for player in players_data:
                # Generate proper match_player_id
                match_player_id = f"mp_{uuid.uuid4().hex[:8]}"

                # Resolve existing player_id
                cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player["player_name"],))
                result = cursor.fetchone()
                existing_player_id = result[0] if result else None

                # Insert roster data
                insert_sql = """
                    INSERT INTO match_player (
                        match_player_id, match_id, player_id, player_name, team_id, 
                        shirt_number, minutes_played
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """

                values = (
                    match_player_id,
                    player["match_id"],
                    existing_player_id,
                    player["player_name"],
                    player["team_id"],
                    player["shirt_number"],
                    player["minutes_played"],
                )

                cursor.execute(insert_sql, values)
                inserted_count += 1

            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"❌ Database insertion error: {e}")
            return False

    def generate_final_summary(self):
        """Generate final extraction summary"""

        elapsed = time.time() - self.start_time

        logger.info(f"\n{'='*60}")
        logger.info(f"BATCH EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(MATCH_IDS)}")
        logger.info(f"✅ Successful extractions: {len(self.successful_matches)}")
        logger.info(f"❌ Failed extractions: {len(self.failed_matches)}")
        logger.info(f"👥 Total players extracted: {self.total_players_extracted}")

        if self.successful_matches:
            logger.info(
                f"📊 Average players per match: {self.total_players_extracted/len(self.successful_matches):.1f}"
            )
            logger.info(f"⚡ Processing rate: {len(self.successful_matches)/elapsed*60:.1f} matches/min")

        if self.failed_matches:
            logger.info("\n❌ Failed matches:")
            for match_id, reason in self.failed_matches[:10]:  # Show first 10 failures
                logger.info(f"  {match_id}: {reason}")
            if len(self.failed_matches) > 10:
                logger.info(f"  ... and {len(self.failed_matches) - 10} more")

        logger.info(f"{'='*60}")

        return self.generate_summary()

    def generate_summary(self):
        """Generate extraction summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0

        return {
            "total_matches": len(MATCH_IDS),
            "processed_matches": self.processed_matches,
            "successful_matches": len(self.successful_matches),
            "failed_matches": len(self.failed_matches),
            "total_players_extracted": self.total_players_extracted,
            "elapsed_time": elapsed,
            "success_rate": len(self.successful_matches) / self.processed_matches * 100
            if self.processed_matches > 0
            else 0,
            "processing_rate": len(self.successful_matches) / elapsed * 60 if elapsed > 0 else 0,
            "failed_match_details": self.failed_matches,
        }


def main():
    """Main function to run batch extraction"""

    extractor = BatchRosterExtractor()
    results = extractor.process_all_matches()

    # Final verification
    logger.info("\n🔍 FINAL VERIFICATION:")
    conn = sqlite3.connect("/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
    cursor = conn.cursor()

    placeholders = ",".join(["?" for _ in MATCH_IDS])
    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT match_id) as matches_with_data,
               COUNT(*) as total_players,
               COUNT(CASE WHEN shirt_number IS NOT NULL THEN 1 END) as players_with_shirt_numbers,
               COUNT(CASE WHEN minutes_played IS NOT NULL THEN 1 END) as players_with_minutes,
               COUNT(CASE WHEN player_id IS NOT NULL THEN 1 END) as players_with_links
        FROM match_player 
        WHERE match_id IN ({placeholders})
    """,
        MATCH_IDS,
    )

    matches_with_data, total_players, with_shirts, with_minutes, with_links = cursor.fetchone()

    logger.info(f"Matches with roster data: {matches_with_data}/{len(MATCH_IDS)}")
    logger.info(f"Total players in database: {total_players}")
    logger.info(f"Players with shirt numbers: {with_shirts} ({with_shirts/total_players*100:.1f}%)")
    logger.info(f"Players with minutes: {with_minutes} ({with_minutes/total_players*100:.1f}%)")
    logger.info(f"Players with ID links: {with_links} ({with_links/total_players*100:.1f}%)")

    conn.close()

    return results


if __name__ == "__main__":
    results = main()
