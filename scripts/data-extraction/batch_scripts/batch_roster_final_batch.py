#!/usr/bin/env python3
"""
Final batch roster extraction for remaining match IDs
Using the proven FIXED methodology
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
        logging.FileHandler("/Users/thomasmcmillan/projects/nwsl_data/final_batch_extraction.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Final batch of match IDs to process
FINAL_BATCH_MATCH_IDS = [
    "4daf6a3e",
    "6bb1dcb0",
    "e6cd7493",
    "570d927f",
    "119e0735",
    "77024d19",
    "444a7265",
    "36a183f3",
    "1b3fed3f",
    "76a963e6",
    "038d5f72",
    "1a60c64d",
    "af9e6aa0",
    "973b8702",
    "dfcc80f1",
    "ecdb0756",
    "f048e50d",
    "58f01846",
    "1c170723",
    "0b33d8bf",
    "7b1d9e98",
    "12ddcc67",
    "7cdb0bb5",
    "b5199fe1",
    "1a157196",
    "1ac67a32",
    "76a4eaa9",
    "82ad937c",
    "62a51875",
    "2e307145",
    "233953db",
    "a3f1fd57",
    "7a39bead",
    "f200e21d",
    "6fab8a8e",
    "4de26c64",
    "1e61e6d0",
    "c5929ccf",
    "6afadaae",
    "9a3194b6",
    "b7d732c1",
    "355edc62",
    "8137d4d4",
    "c00c0e92",
    "7247e49a",
    "290df075",
    "8d3c0bbb",
    "cc101301",
    "9bb6829b",
    "85f65da4",
    "8c81b41c",
    "809a38d9",
    "dcce831d",
    "2b8626c8",
    "4fc7c81b",
    "efd663a7",
    "26ad3f02",
    "b785dc25",
    "75cae5a2",
    "799c6310",
    "84320d5c",
    "ac1441ed",
    "bb5b4d67",
    "9243d508",
    "aa5de61c",
    "2a629ec8",
    "70633eda",
    "0a18ef58",
    "800d40e3",
    "382cc9a5",
    "19671f44",
    "faedb1cf",
    "cb2cf7c8",
    "231fb934",
    "6bf69bec",
    "cf8ab658",
    "e094b273",
    "87ec8077",
    "873ded94",
    "42354974",
    "51c088f0",
    "ef2ef72c",
    "d2bd367f",
    "557b86ec",
    "8f0b38be",
    "6ef26dfb",
    "2a94c73f",
    "78a1130b",
    "9f9f9f44",
    "da63ca52",
    "1da7faf9",
    "8dea19d0",
    "4366d18b",
    "ce23a8c3",
    "86457d5c",
    "bd44bb43",
    "6042fdb4",
    "0fc70274",
    "b9858345",
    "cc8f4050",
    "0359e2a8",
    "02cc2db8",
    "32b74c42",
    "8777bb7e",
    "f80ba5a9",
    "a37d3cfd",
    "11764aec",
    "eca1e88a",
    "22ee2a8e",
    "3b0958a3",
    "1c672cf2",
    "a1dc77b2",
    "f88cc7e3",
    "bb90432b",
    "22b9918f",
    "0c66c1c4",
    "9d816b1c",
    "9467b1af",
    "42534159",
    "076238c6",
    "e0c9fd2f",
    "807c9e51",
    "6546a002",
    "63f04e4d",
    "162e58e3",
    "ffe3a6b6",
    "5b076473",
    "9d04e156",
    "a4476197",
    "24e15195",
    "6dc65c5d",
    "f9e04c6f",
    "2de27dd8",
    "f2f758a1",
    "d6b5ef91",
    "5cc2c4e6",
    "9d2567b5",
    "f862299e",
    "f6c8d2fc",
    "5c41b080",
    "2c708d10",
    "42af74d7",
    "824e6399",
    "e9749264",
    "27c148ec",
    "32e46224",
    "35872cab",
    "dd811060",
    "e229e7d5",
    "fa838384",
    "9cd54811",
    "7c21b4c6",
    "4a41b9cc",
    "14d99e42",
    "23954a43",
    "907eb79c",
    "7aa8dbb7",
    "4d47a98b",
    "7b8a2deb",
    "2bd69279",
    "c0e04bc8",
    "6f5bbb39",
    "09f3121e",
    "caf64d89",
    "276ca70d",
    "9f2f3d48",
    "a9fb7f83",
    "ac9e20a5",
    "3b03fa77",
    "f16c843b",
    "32fb4d07",
    "7a097c88",
    "d4d39b88",
    "2efb993b",
    "bcb0e7fb",
    "2b8f9b59",
]


class FinalBatchRosterExtractor:
    """Final batch extract roster data using proven methodology"""

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
        """Process all final batch match IDs"""

        self.start_time = time.time()

        logger.info("🚀 Starting FINAL batch roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(FINAL_BATCH_MATCH_IDS)}")

        # Check seasons and existing data
        self.analyze_matches()

        # Check which matches already have data
        existing_matches = self.check_existing_data()

        matches_to_process = []
        for match_id in FINAL_BATCH_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)

        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(FINAL_BATCH_MATCH_IDS) - len(matches_to_process)}")

        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()

        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING FINAL BATCH EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))

        return self.generate_final_summary()

    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        placeholders = ",".join(["?" for _ in FINAL_BATCH_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """,
            FINAL_BATCH_MATCH_IDS,
        )

        season_breakdown = cursor.fetchall()

        logger.info("📊 Season breakdown of final batch matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count

        if total_found < len(FINAL_BATCH_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(FINAL_BATCH_MATCH_IDS)} matches found in database")

        conn.close()

    def check_existing_data(self):
        """Check which matches already have roster data"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get matches that already have roster data
        placeholders = ",".join(["?" for _ in FINAL_BATCH_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """,
            FINAL_BATCH_MATCH_IDS,
        )

        existing_data = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        logger.info("📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(FINAL_BATCH_MATCH_IDS)}")

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
        logger.info(f"FINAL BATCH EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(FINAL_BATCH_MATCH_IDS)}")
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
            "total_matches": len(FINAL_BATCH_MATCH_IDS),
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
    """Main function to run final batch extraction"""

    extractor = FinalBatchRosterExtractor()
    results = extractor.process_all_matches()

    # Comprehensive final verification
    logger.info("\n🔍 COMPREHENSIVE FINAL VERIFICATION:")
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

    logger.info("🎯 OVERALL DATABASE STATUS:")
    logger.info(f"Seasons with roster data: {seasons}")
    logger.info(f"Total matches with roster data: {matches}")
    logger.info(f"Total players in database: {total_players}")
    logger.info(f"Players with shirt numbers: {with_shirts} ({with_shirts/total_players*100:.1f}%)")
    logger.info(f"Players with minutes: {with_minutes} ({with_minutes/total_players*100:.1f}%)")
    logger.info(f"Players with ID links: {with_links} ({with_links/total_players*100:.1f}%)")
    logger.info(f"Players with season_id: {with_season} ({with_season/total_players*100:.1f}%)")

    # Get season breakdown
    cursor.execute("""
        SELECT s.season_year, COUNT(DISTINCT mp.match_id) as matches, COUNT(*) as players
        FROM match_player mp 
        JOIN match m ON mp.match_id = m.match_id 
        JOIN season s ON m.season_id = s.season_id 
        GROUP BY s.season_year 
        ORDER BY s.season_year DESC
    """)

    season_breakdown = cursor.fetchall()

    logger.info("\n📊 SEASON BREAKDOWN:")
    for season_year, match_count, player_count in season_breakdown:
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players")

    conn.close()

    return results


if __name__ == "__main__":
    results = main()
