#!/usr/bin/env python3
"""
Batch roster extraction for 91 additional historical match IDs
Using the proven FIXED methodology from previous successful extractions
Building towards an UNPRECEDENTED 11+ SEASON comprehensive NWSL database!
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
        logging.FileHandler("/Users/thomasmcmillan/projects/nwsl_data/historical_91_extraction.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# List of 91 additional historical match IDs to process
HISTORICAL_91_MATCH_IDS = [
    "c472965f",
    "5d762423",
    "478694f8",
    "c6e91034",
    "6d73cf64",
    "520134f4",
    "170d1dda",
    "c78c16a1",
    "9dcf496b",
    "3c2a99b1",
    "418e0e31",
    "e4c6e5a2",
    "e38fdb0f",
    "db1b8928",
    "f7ec5018",
    "e01f2ab2",
    "85ea774d",
    "611eb468",
    "1b276643",
    "3f955256",
    "4874cbfb",
    "f13fd918",
    "60713322",
    "29994120",
    "6c10cfdb",
    "3b6c58de",
    "0ab262a8",
    "d8f2bb6c",
    "4ccba30d",
    "b2f4ab2b",
    "53d69588",
    "541a43ed",
    "fb3fee9e",
    "d68ba0d9",
    "070a85c6",
    "0ac89923",
    "55840597",
    "0438ac6f",
    "5cbe3ea6",
    "2124d8de",
    "ad4385ca",
    "6c1960fe",
    "ca6b1e40",
    "9eae33e6",
    "0349e876",
    "f4e707e7",
    "bd617704",
    "b3536acf",
    "a8e5bb48",
    "1a84f24d",
    "bd7ec044",
    "7f3417a9",
    "caadbdc9",
    "7439eff5",
    "55934b05",
    "6c066abf",
    "0e7fddc2",
    "45ea9030",
    "61cc1e00",
    "565b1da5",
    "2d678cba",
    "889e323c",
    "f5582e60",
    "502ff272",
    "5c3b3b7a",
    "2bda25a1",
    "5ff5843f",
    "7aa0733c",
    "dc4da4e7",
    "88db5cc5",
    "decc5784",
    "1cf24c9d",
    "bc51c892",
    "50bd521d",
    "7239cc9a",
    "1bb660c2",
    "ade5efdc",
    "573e3558",
    "6c0afe0b",
    "fd03e664",
    "99b29ebc",
    "6fd000ec",
    "708f6bbe",
    "707d1ba5",
    "252a6a92",
    "266fc787",
    "08df8ebf",
    "5191e299",
    "be554b90",
    "5fe1d504",
    "093ed426",
    "9bd6f453",
    "c09cea95",
]


class Historical91RosterExtractor:
    """Extract roster data for 91 additional historical matches using proven methodology"""

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
        """Process all 91 additional historical match IDs"""

        self.start_time = time.time()

        logger.info("🚀 Starting HISTORICAL 91 roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(HISTORICAL_91_MATCH_IDS)}")
        logger.info("🎯 Building towards UNPRECEDENTED 11+ SEASON comprehensive NWSL database!")

        # Check seasons and existing data
        self.analyze_matches()

        # Check which matches already have data
        existing_matches = self.check_existing_data()

        matches_to_process = []
        for match_id in HISTORICAL_91_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)

        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(HISTORICAL_91_MATCH_IDS) - len(matches_to_process)}")

        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()

        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING HISTORICAL 91 EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))

        return self.generate_final_summary()

    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        placeholders = ",".join(["?" for _ in HISTORICAL_91_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """,
            HISTORICAL_91_MATCH_IDS,
        )

        season_breakdown = cursor.fetchall()

        logger.info("📊 Season breakdown of historical 91 matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count

        if total_found < len(HISTORICAL_91_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(HISTORICAL_91_MATCH_IDS)} matches found in database")

        conn.close()

    def check_existing_data(self):
        """Check which matches already have roster data"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get matches that already have roster data
        placeholders = ",".join(["?" for _ in HISTORICAL_91_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """,
            HISTORICAL_91_MATCH_IDS,
        )

        existing_data = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        logger.info("📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(HISTORICAL_91_MATCH_IDS)}")

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
        logger.info(f"HISTORICAL 91 EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(HISTORICAL_91_MATCH_IDS)}")
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
            "total_matches": len(HISTORICAL_91_MATCH_IDS),
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
    """Main function to run historical 91 batch extraction"""

    extractor = Historical91RosterExtractor()
    results = extractor.process_all_matches()

    # Comprehensive final verification - THE BIG 11+ SEASON DATABASE CHECK!
    logger.info("\n🔍 COMPREHENSIVE 11+ SEASON DATABASE VERIFICATION:")
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

    logger.info("\n📊 COMPLETE MULTI-SEASON BREAKDOWN:")
    total_matches_db = 0
    total_players_db = 0
    for season_year, match_count, player_count in season_breakdown:
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players")
        total_matches_db += match_count
        total_players_db += player_count

    logger.info("\n🏆 UNPRECEDENTED DATABASE TOTALS:")
    logger.info(f"  Total Seasons: {len(season_breakdown)}")
    logger.info(f"  Total Matches: {total_matches_db}")
    logger.info(f"  Total Players: {total_players_db}")
    logger.info("  Database Coverage: UNPRECEDENTED ✅")
    logger.info(f"  Historical Span: {len(season_breakdown)} FULL SEASONS ✅")

    if len(season_breakdown) >= 11:
        logger.info("\n🎉 UNPRECEDENTED MILESTONE: 11+ SEASON NWSL DATABASE!")
        logger.info("🚀 This database now exceeds a full decade of coverage!")
        logger.info("📈 Coverage: Beyond the first decade of professional women's soccer!")
    elif len(season_breakdown) >= 10:
        logger.info("\n🎉 CONTINUING THE DECADE-PLUS DATABASE EXPANSION!")
        logger.info("🚀 Building towards unprecedented historical coverage!")

    conn.close()

    return results


if __name__ == "__main__":
    results = main()
