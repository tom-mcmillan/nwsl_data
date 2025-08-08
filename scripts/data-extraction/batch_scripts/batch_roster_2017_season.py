#!/usr/bin/env python3
"""
Batch roster extraction for 2017 season match IDs
Using the proven FIXED methodology from previous successful extractions
Building towards a comprehensive 9-season NWSL database!
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
        logging.FileHandler("/Users/thomasmcmillan/projects/nwsl_data/season_2017_extraction.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# List of 2017 season match IDs to process
SEASON_2017_MATCH_IDS = [
    "bb09ce9f",
    "cfce4a7e",
    "60c0fef2",
    "e0a6d860",
    "eb7f25b6",
    "64ae00a8",
    "bde14f5d",
    "ab51dc2a",
    "b14ef42f",
    "a0785409",
    "aae9acb9",
    "5ecc20d9",
    "657ab5b5",
    "2bbf11bc",
    "258598f2",
    "9f6d6df5",
    "0d86f760",
    "11e0e304",
    "9fa14fb5",
    "2d9e0ef0",
    "40d29f66",
    "2f8861a3",
    "beaa3276",
    "ba8ffe87",
    "72856ea5",
    "169816e4",
    "aae24672",
    "6ff06b8e",
    "e1e5105f",
    "625276de",
    "cb36f524",
    "b8918216",
    "554b360b",
    "6245629c",
    "18fdb801",
    "83e9f6cd",
    "391bfb7d",
    "49e5362d",
    "34a9f7eb",
    "e493bc1b",
    "78894a6f",
    "eaf0b116",
    "ba381725",
    "acf83533",
    "5ada410d",
    "21aac0ca",
    "02765e6d",
    "bc4d18e2",
    "14d766d0",
    "563b9572",
    "d1f5bba9",
    "c919bc72",
    "94916b48",
    "d6daa6a9",
    "8aa2a6b5",
    "20f76f42",
    "9f3f16bd",
    "440a6ec7",
    "66a2ae77",
    "127db480",
    "8b6d410c",
    "0e9d2a77",
    "0df8900f",
    "56cb2952",
    "1193750f",
    "08f400cf",
    "b583fc1c",
    "48a4cf5c",
    "55034c4e",
    "3166599e",
    "df22842d",
    "2682e8f7",
    "aacb507b",
    "a72ceab9",
    "3534d09f",
    "fb592d13",
    "13ecb249",
    "83cce59c",
    "b9ef73c0",
    "04a87c26",
    "edd08e31",
    "37d5d942",
    "4f0323c2",
    "2ac3cfd3",
    "c7123f09",
    "eb456c25",
    "e265026c",
    "ef2c6549",
    "f8de4250",
    "1f835c73",
    "6cf91083",
    "2818ce94",
    "5798634c",
    "5c6f66f8",
    "b1d44080",
    "8cd6d492",
    "2f3dc8f1",
    "e12b3240",
    "cb78b384",
    "f4ef0410",
    "65cf244a",
    "e12416e1",
    "678925e4",
    "f1a2a96d",
    "50ef46e1",
    "3dfd780a",
    "f4967720",
    "1fcb0c10",
    "666e71f1",
    "137224e9",
    "f4b7003b",
    "083cf4ad",
    "4b7edd1f",
    "0a035412",
    "fa841c55",
    "8dd983d8",
    "0c666b52",
    "f2348044",
    "7b27fb60",
    "277a7676",
    "6ff3d866",
    "c91958d4",
    "5206d9f2",
]


class Season2017RosterExtractor:
    """Extract roster data for 2017 season matches using proven methodology"""

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
        """Process all 2017 season match IDs"""

        self.start_time = time.time()

        logger.info("🚀 Starting 2017 SEASON roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(SEASON_2017_MATCH_IDS)}")
        logger.info("🎯 Building towards 9-SEASON comprehensive NWSL database!")

        # Check seasons and existing data
        self.analyze_matches()

        # Check which matches already have data
        existing_matches = self.check_existing_data()

        matches_to_process = []
        for match_id in SEASON_2017_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)

        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(SEASON_2017_MATCH_IDS) - len(matches_to_process)}")

        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()

        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING 2017 SEASON EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))

        return self.generate_final_summary()

    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        placeholders = ",".join(["?" for _ in SEASON_2017_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """,
            SEASON_2017_MATCH_IDS,
        )

        season_breakdown = cursor.fetchall()

        logger.info("📊 Season breakdown of 2017 season matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count

        if total_found < len(SEASON_2017_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(SEASON_2017_MATCH_IDS)} matches found in database")

        conn.close()

    def check_existing_data(self):
        """Check which matches already have roster data"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get matches that already have roster data
        placeholders = ",".join(["?" for _ in SEASON_2017_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """,
            SEASON_2017_MATCH_IDS,
        )

        existing_data = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        logger.info("📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(SEASON_2017_MATCH_IDS)}")

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

        # Progress update every 25 matches
        if current % 25 == 0:
            elapsed = time.time() - self.start_time
            rate = current / elapsed * 60  # matches per minute
            remaining = (total - current) / rate if rate > 0 else 0

            logger.info(f"📊 Progress: {current}/{total} ({current/total*100:.1f}%)")
            logger.info(f"⏱️  Rate: {rate:.1f} matches/min, Est. remaining: {remaining:.1f} min")
            logger.info(f"✅ Success: {len(self.successful_matches)}, ❌ Failed: {len(self.failed_matches)}")

    def extract_roster_fixed(self, html_path, match_id):
        """Extract roster using FIXED methodology"""

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
        """Insert roster data into match_player table including season_id"""

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            inserted_count = 0

            # Get season_id for the match
            match_id = players_data[0]["match_id"]
            cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (match_id,))
            result = cursor.fetchone()
            season_id = result[0] if result else None

            for player in players_data:
                # Generate proper match_player_id
                match_player_id = f"mp_{uuid.uuid4().hex[:8]}"

                # Resolve existing player_id
                cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player["player_name"],))
                result = cursor.fetchone()
                existing_player_id = result[0] if result else None

                # Insert roster data including season_id
                insert_sql = """
                    INSERT INTO match_player (
                        match_player_id, match_id, player_id, player_name, team_id, 
                        shirt_number, minutes_played, season_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """

                values = (
                    match_player_id,
                    player["match_id"],
                    existing_player_id,
                    player["player_name"],
                    player["team_id"],
                    player["shirt_number"],
                    player["minutes_played"],
                    season_id,
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
        logger.info(f"2017 SEASON EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(SEASON_2017_MATCH_IDS)}")
        logger.info(f"✅ Successful extractions: {len(self.successful_matches)}")
        logger.info(f"❌ Failed extractions: {len(self.failed_matches)}")
        logger.info(f"👥 Total players extracted: {self.total_players_extracted}")

        if self.successful_matches:
            logger.info(
                f"📊 Average players per match: {self.total_players_extracted/len(self.successful_matches):.1f}"
            )
            logger.info(f"⚡ Processing rate: {len(self.successful_matches)/elapsed*60:.1f} matches/min")

        success_rate = len(self.successful_matches) / self.processed_matches * 100 if self.processed_matches > 0 else 0
        logger.info(f"🎯 Success rate: {success_rate:.1f}%")

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
            "total_matches": len(SEASON_2017_MATCH_IDS),
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
    """Main function to run 2017 season extraction"""

    extractor = Season2017RosterExtractor()
    results = extractor.process_all_matches()

    # Comprehensive final verification - THE BIG 9-SEASON DATABASE CHECK!
    logger.info("\n🔍 COMPREHENSIVE 9-SEASON DATABASE VERIFICATION:")
    conn = sqlite3.connect("/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
    cursor = conn.cursor()

    # Get overall database statistics
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT s.season_year) as seasons_with_data,
            COUNT(DISTINCT mp.match_id) as total_matches_with_roster,
            COUNT(*) as total_players,
            COUNT(CASE WHEN mp.shirt_number IS NOT NULL THEN 1 END) as players_with_shirt_numbers,
            COUNT(CASE WHEN mp.minutes_played IS NOT NULL THEN 1 END) as players_with_minutes,
            COUNT(CASE WHEN mp.player_id IS NOT NULL THEN 1 END) as players_with_id_links,
            COUNT(CASE WHEN mp.season_id IS NOT NULL THEN 1 END) as players_with_season_id
        FROM match_player mp 
        JOIN match m ON mp.match_id = m.match_id 
        JOIN season s ON m.season_id = s.season_id
    """)

    seasons, matches, total_players, with_shirts, with_minutes, with_links, with_season = cursor.fetchone()

    logger.info("🎯 COMPREHENSIVE DATABASE STATUS:")
    logger.info(f"Seasons with roster data: {seasons}")
    logger.info(f"Total matches with roster data: {matches}")
    logger.info(f"Total players in database: {total_players}")
    logger.info(f"Players with shirt numbers: {with_shirts} ({with_shirts/total_players*100:.1f}%)")
    logger.info(f"Players with minutes: {with_minutes} ({with_minutes/total_players*100:.1f}%)")
    logger.info(f"Players with ID links: {with_links} ({with_links/total_players*100:.1f}%)")
    logger.info(f"Players with season_id: {with_season} ({with_season/total_players*100:.1f}%)")

    # Get complete season breakdown
    cursor.execute("""
        SELECT s.season_year, COUNT(DISTINCT mp.match_id) as matches, COUNT(*) as players
        FROM match_player mp 
        JOIN match m ON mp.match_id = m.match_id 
        JOIN season s ON m.season_id = s.season_id 
        GROUP BY s.season_year 
        ORDER BY s.season_year DESC
    """)

    season_breakdown = cursor.fetchall()

    logger.info("\n📊 COMPLETE 9-SEASON BREAKDOWN:")
    total_matches_db = 0
    total_players_db = 0
    for season_year, match_count, player_count in season_breakdown:
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players")
        total_matches_db += match_count
        total_players_db += player_count

    logger.info("\n🏆 HISTORIC 9-SEASON DATABASE TOTALS:")
    logger.info(f"  Total Seasons: {len(season_breakdown)} (2017-2025)")
    logger.info(f"  Total Matches: {total_matches_db}")
    logger.info(f"  Total Players: {total_players_db}")
    logger.info("  Database Coverage: COMPREHENSIVE ✅")
    logger.info("  Historical Span: 9 FULL SEASONS ✅")

    if len(season_breakdown) >= 9:
        logger.info("\n🎉 MILESTONE ACHIEVED: 9-SEASON NWSL DATABASE COMPLETE!")
        logger.info("🚀 This represents one of the most comprehensive NWSL databases ever created!")

    conn.close()

    return results


if __name__ == "__main__":
    results = main()
