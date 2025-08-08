#!/usr/bin/env python3
"""
Batch roster extraction for additional match IDs
Using the proven FIXED methodology from 2025 season extraction
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
        logging.FileHandler("/Users/thomasmcmillan/projects/nwsl_data/additional_roster_extraction.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# List of additional match IDs to process
ADDITIONAL_MATCH_IDS = [
    "9ad58931",
    "580abedf",
    "cab0661f",
    "d41fc789",
    "96e0dd2c",
    "3075b5c0",
    "0a54da3b",
    "1670d17a",
    "737678f8",
    "a7d01063",
    "d91aea7c",
    "65d57a30",
    "528bbf25",
    "a7daf861",
    "648bb908",
    "8268db6d",
    "15580ec7",
    "233e11eb",
    "50db38c8",
    "9f02aad1",
    "dd1430c9",
    "ffef1d9a",
    "38101d6b",
    "bbd9f51a",
    "4d4843ca",
    "554d9958",
    "0ca01c78",
    "f337712c",
    "d5c6cdab",
    "43c5ccd9",
    "2d5641de",
    "3be42001",
    "a384fbda",
    "694bd5f6",
    "a36e071c",
    "65e6a4bd",
    "ab5877a7",
    "2cd5aa58",
    "dfb90a73",
    "50e71049",
    "034925ae",
    "75606d80",
    "ef84043d",
    "7d118693",
    "e3f72dbe",
    "6556c637",
    "65ed77f0",
    "b792c346",
    "22dc6222",
    "d2575048",
    "3bb46cff",
    "98086391",
    "28e6c7a2",
    "98440caf",
    "1b8a084e",
    "877d362d",
    "c1cac8e6",
    "11c32fb2",
    "307d7dd2",
    "da18f85c",
    "ad773c94",
    "a263cad4",
    "5363a84e",
    "b2023bde",
    "cff46fe3",
    "7b7e3b0c",
    "c52cb315",
    "8e441f7c",
    "1bfbde52",
    "f0b371c6",
    "5c48f9e2",
    "b7d2f49c",
    "44143f63",
    "d2234462",
    "213998de",
    "969d88d7",
    "131887e6",
    "3a943218",
    "1046f207",
    "d0ef16cc",
    "645bd1f9",
    "f9e94321",
    "1ea954ae",
    "ce065b4e",
    "116736c1",
    "26aaa42e",
    "5c78295c",
    "e191975f",
    "a1fcd4e5",
    "30ade4e0",
    "b81af036",
    "621bc391",
    "e08d6c4f",
    "271bb187",
    "2aef3feb",
    "a2773d43",
    "5bb56387",
    "402a27f7",
    "8459babf",
    "58602728",
    "af0d4c1d",
    "b8fbf719",
    "376754a4",
    "39382fed",
    "db34abb9",
    "5cdb5294",
    "d823665c",
    "c1ec23ab",
    "cfe0e5e2",
    "347fc889",
    "99f75387",
    "8882ca01",
    "6122aad9",
    "ffc88eef",
    "ddb3f581",
    "fa4654dc",
    "ab4a9a85",
    "fada5342",
    "097dd81e",
    "accfd699",
    "e0cba9a0",
    "0bbc1d3a",
    "ce4d64c6",
    "0be54aa1",
    "17572b77",
    "123f2632",
    "a3655313",
    "39ed7c23",
    "bf1bed18",
    "0fc05a78",
    "3d9812c7",
    "28b5fbd4",
    "abd75002",
    "8798b399",
    "bcf89d77",
    "e4e02187",
    "71d44dd8",
    "264e1580",
    "72b85d57",
    "c26dbae0",
    "183a6c5b",
    "f55c85b3",
    "9ceee191",
    "4eb8c7b7",
    "0c5c2895",
    "8863fbca",
    "7ef32f41",
    "96b60abd",
    "1cf79406",
    "6e2ba6e7",
    "3d0b5a1d",
    "a7005232",
    "18d35e73",
    "a6984338",
    "96b3955e",
    "771c3b61",
    "1e837a99",
    "e8f52f26",
    "35bd2a13",
    "cd89bcfb",
    "3f2e2d3f",
    "557ce1c6",
    "a51234f5",
    "9c97389e",
    "d6f7c3b8",
    "0e889e55",
    "cd6d5b4f",
    "bfd1ee7c",
    "c08ebbce",
    "9eace85e",
    "664107de",
    "2164c947",
    "03f56ad5",
    "15d59878",
    "cb653ae5",
    "0603e751",
    "7d3be8a8",
    "da8e116e",
    "73672411",
    "f87a6d18",
    "d437189b",
    "5a2a2730",
    "66eef21d",
    "5fef737e",
    "70f6ed43",
    "e96c892d",
    "87c96d20",
    "d711b57e",
    "f168f5b1",
    "5a808fa8",
]


class AdditionalRosterExtractor:
    """Extract roster data for additional matches using proven methodology"""

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
        """Process all additional match IDs"""

        self.start_time = time.time()

        logger.info("🚀 Starting additional roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(ADDITIONAL_MATCH_IDS)}")

        # Check seasons and existing data
        self.analyze_matches()

        # Check which matches already have data
        existing_matches = self.check_existing_data()

        matches_to_process = []
        for match_id in ADDITIONAL_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)

        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(ADDITIONAL_MATCH_IDS) - len(matches_to_process)}")

        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()

        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING ADDITIONAL EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))

        return self.generate_final_summary()

    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        placeholders = ",".join(["?" for _ in ADDITIONAL_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """,
            ADDITIONAL_MATCH_IDS,
        )

        season_breakdown = cursor.fetchall()

        logger.info("📊 Season breakdown of additional matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count

        if total_found < len(ADDITIONAL_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(ADDITIONAL_MATCH_IDS)} matches found in database")

        conn.close()

    def check_existing_data(self):
        """Check which matches already have roster data"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get matches that already have roster data
        placeholders = ",".join(["?" for _ in ADDITIONAL_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """,
            ADDITIONAL_MATCH_IDS,
        )

        existing_data = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        logger.info("📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(ADDITIONAL_MATCH_IDS)}")

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

        # Progress update every 20 matches
        if current % 20 == 0:
            elapsed = time.time() - self.start_time
            rate = current / elapsed * 60  # matches per minute
            remaining = (total - current) / rate if rate > 0 else 0

            logger.info(f"📊 Progress: {current}/{total} ({current/total*100:.1f}%)")
            logger.info(f"⏱️  Rate: {rate:.1f} matches/min, Est. remaining: {remaining:.1f} min")
            logger.info(f"✅ Success: {len(self.successful_matches)}, ❌ Failed: {len(self.failed_matches)}")

    def extract_roster_fixed(self, html_path, match_id):
        """Extract roster using FIXED methodology from successful 2025 extraction"""

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
        """Insert roster data into match_player table with season_id"""

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
        logger.info(f"ADDITIONAL EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(ADDITIONAL_MATCH_IDS)}")
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
            "total_matches": len(ADDITIONAL_MATCH_IDS),
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
    """Main function to run additional batch extraction"""

    extractor = AdditionalRosterExtractor()
    results = extractor.process_all_matches()

    # Final verification
    logger.info("\n🔍 FINAL VERIFICATION:")
    conn = sqlite3.connect("/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
    cursor = conn.cursor()

    placeholders = ",".join(["?" for _ in ADDITIONAL_MATCH_IDS])
    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT mp.match_id) as matches_with_data,
               COUNT(*) as total_players,
               COUNT(CASE WHEN shirt_number IS NOT NULL THEN 1 END) as players_with_shirt_numbers,
               COUNT(CASE WHEN minutes_played IS NOT NULL THEN 1 END) as players_with_minutes,
               COUNT(CASE WHEN player_id IS NOT NULL THEN 1 END) as players_with_links,
               COUNT(CASE WHEN season_id IS NOT NULL THEN 1 END) as players_with_season_id
        FROM match_player mp
        WHERE mp.match_id IN ({placeholders})
    """,
        ADDITIONAL_MATCH_IDS,
    )

    matches_with_data, total_players, with_shirts, with_minutes, with_links, with_season = cursor.fetchone()

    logger.info(f"Matches with roster data: {matches_with_data}/{len(ADDITIONAL_MATCH_IDS)}")
    logger.info(f"Total players in database: {total_players}")
    logger.info(f"Players with shirt numbers: {with_shirts} ({with_shirts/total_players*100:.1f}%)")
    logger.info(f"Players with minutes: {with_minutes} ({with_minutes/total_players*100:.1f}%)")
    logger.info(f"Players with ID links: {with_links} ({with_links/total_players*100:.1f}%)")
    logger.info(f"Players with season_id: {with_season} ({with_season/total_players*100:.1f}%)")

    conn.close()

    return results


if __name__ == "__main__":
    results = main()
