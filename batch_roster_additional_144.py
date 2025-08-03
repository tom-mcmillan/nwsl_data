#!/usr/bin/env python3
"""
Batch roster extraction for 144 additional match IDs
Using the proven FIXED methodology from previous successful extractions
"""

import sqlite3
import os
import uuid
import time
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/thomasmcmillan/projects/nwsl_data/batch_144_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# List of 144 additional match IDs to process
ADDITIONAL_144_MATCH_IDS = [
    "92ccc792", "bf8c7152", "2e23bf3e", "14b12758", "072e422f", "78660c17", "76f87d7e", "c1330700",
    "b34794ed", "6ba30741", "b23322de", "9733a93c", "a5d7e67a", "28529cef", "be723b18", "2184074e",
    "adffaaa7", "0ec1b5bb", "fb5d227f", "b161f09c", "c19e2edc", "55fd0afd", "5a27b398", "7d903802",
    "8acfa339", "74666aca", "b818bb1d", "1011f557", "5648dd0c", "671d9115", "0f623827", "9f0e821c",
    "5dceef91", "f29a7ef1", "481dcee5", "2cd4ec75", "10f108c3", "40d971e5", "3548ce5a", "db88df87",
    "4ce881eb", "ab0a3b8e", "f8ee8767", "9b3974b4", "fa5f28b6", "08a226bf", "753a54ab", "151a8c6c",
    "138422ad", "986e95dc", "2a4f95fc", "c3692424", "c17893bb", "b2fdb217", "29064988", "9a7e9c60",
    "57127901", "f832fee9", "4d241e61", "80ff5d48", "e45b356c", "5c64fc50", "b4cacaf2", "2b4a0943",
    "a81c893d", "d11a6afc", "fe50bdd7", "800bc864", "2a7aad78", "242d98cb", "4ebf1b38", "b8c01225",
    "8dca4022", "81e7d577", "be46a00f", "714ba654", "aff251d2", "3ea31ead", "16e23f3c", "8d282de4",
    "2f020390", "8eb3b0dd", "0bfe8e38", "7fc53fb8", "fea90630", "d6191f62", "770ec49c", "165448bc",
    "509b89aa", "37d40689", "0bbd1959", "84de42bc", "94bcf409", "331b8375", "b2c4fa2c", "d887c826",
    "343ae440", "a6d4d95a", "b599c7e0", "6b5d492c", "5d320b9e", "74f63303", "e8cf14bf", "3f95b1ba",
    "fc2a3172", "5f985550", "dc4570b2", "b7041740", "2b50579e", "b5eddd77", "9d5e9ab5", "e62b6c8a",
    "c364a795", "1e851026", "500515e8", "14675a38", "94d3e20f", "b9817679", "cc66a2f1", "176d858d",
    "bd3531f7", "725b6b2b", "441fc804", "9211e53e", "d7eebcbd", "051a8bb7", "06d86a41", "24ddd8b3",
    "f0a6559b", "8d61a870", "7a7e6622", "8f77608d", "1d2f9fad", "cd30a848", "7f8b4663", "f6d763ba",
    "b67be4fe", "e97c6f39", "73ff619b", "3f04a185", "57b30c8a", "2a0b287c", "7837616f", "e0ef83c8",
    "888fb121", "119eeed3"
]

class Additional144RosterExtractor:
    """Extract roster data for additional 144 matches using proven methodology"""
    
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
        """Process all additional 144 match IDs"""
        
        self.start_time = time.time()
        
        logger.info(f"🚀 Starting ADDITIONAL 144 batch roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(ADDITIONAL_144_MATCH_IDS)}")
        
        # Check seasons and existing data
        self.analyze_matches()
        
        # Check which matches already have data
        existing_matches = self.check_existing_data()
        
        matches_to_process = []
        for match_id in ADDITIONAL_144_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)
        
        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(ADDITIONAL_144_MATCH_IDS) - len(matches_to_process)}")
        
        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()
        
        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING ADDITIONAL 144 EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        
        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))
        
        return self.generate_final_summary()
    
    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in ADDITIONAL_144_MATCH_IDS])
        cursor.execute(f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """, ADDITIONAL_144_MATCH_IDS)
        
        season_breakdown = cursor.fetchall()
        
        logger.info(f"📊 Season breakdown of additional 144 matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count
        
        if total_found < len(ADDITIONAL_144_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(ADDITIONAL_144_MATCH_IDS)} matches found in database")
        
        conn.close()
    
    def check_existing_data(self):
        """Check which matches already have roster data"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get matches that already have roster data
        placeholders = ','.join(['?' for _ in ADDITIONAL_144_MATCH_IDS])
        cursor.execute(f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """, ADDITIONAL_144_MATCH_IDS)
        
        existing_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        logger.info(f"📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(ADDITIONAL_144_MATCH_IDS)}")
        
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
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Find all tables to identify summary tables
        tables = soup.find_all("table")
        table_ids = [table.get("id") for table in tables if table.get("id")]
        
        # Find summary tables
        summary_tables = [tid for tid in table_ids if 'stats_' in tid and '_summary' in tid]
        
        if not summary_tables:
            logger.warning(f"⚠️  No summary tables found for {match_id}")
            return []
        
        all_players = []
        
        # Process each team's summary table
        for table_id in summary_tables:
            team_id = table_id.split('_')[1]
            table = soup.find('table', id=table_id)
            
            if not table:
                continue
            
            # Convert to DataFrame
            df = pd.read_html(str(table))[0]
            
            # Handle MultiIndex columns with FIXED mapping
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(col).strip() if col[1] else col[0] for col in df.columns.values]
            
            # Process each player row
            for idx, row in df.iterrows():
                player_name = str(row.iloc[0]).strip()
                
                # Filter out invalid rows and team totals
                if (pd.isna(player_name) or 
                    player_name == '' or 
                    player_name.lower() in ['player', 'nan'] or
                    'Players' in player_name):
                    continue
                
                # Extract using FIXED column mapping
                player_data = {
                    'match_id': match_id,
                    'player_name': player_name,
                    'team_id': team_id,
                    'shirt_number': self.safe_extract_int(row, ['Unnamed: 1_level_0_#']),
                    'minutes_played': self.safe_extract_int(row, ['Unnamed: 5_level_0_Min']),
                }
                
                all_players.append(player_data)
        
        return all_players
    
    def safe_extract_int(self, row, possible_columns):
        """Safely extract integer value from row"""
        for col in possible_columns:
            if col in row.index:
                value = row[col]
                if pd.notna(value) and str(value).strip() != '':
                    try:
                        return int(float(str(value).replace(',', '')))
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
            match_id = players_data[0]['match_id']
            cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (match_id,))
            result = cursor.fetchone()
            season_id = result[0] if result else None
            
            for player in players_data:
                # Generate proper match_player_id
                match_player_id = f"mp_{uuid.uuid4().hex[:8]}"
                
                # Resolve existing player_id
                cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player['player_name'],))
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
                    player['match_id'],
                    existing_player_id,
                    player['player_name'],
                    player['team_id'],
                    player['shirt_number'],
                    player['minutes_played'],
                    season_id
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
        logger.info(f"ADDITIONAL 144 EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(ADDITIONAL_144_MATCH_IDS)}")
        logger.info(f"✅ Successful extractions: {len(self.successful_matches)}")
        logger.info(f"❌ Failed extractions: {len(self.failed_matches)}")
        logger.info(f"👥 Total players extracted: {self.total_players_extracted}")
        
        if self.successful_matches:
            logger.info(f"📊 Average players per match: {self.total_players_extracted/len(self.successful_matches):.1f}")
            logger.info(f"⚡ Processing rate: {len(self.successful_matches)/elapsed*60:.1f} matches/min")
        
        if self.failed_matches:
            logger.info(f"\n❌ Failed matches:")
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
            'total_matches': len(ADDITIONAL_144_MATCH_IDS),
            'processed_matches': self.processed_matches,
            'successful_matches': len(self.successful_matches),
            'failed_matches': len(self.failed_matches),
            'total_players_extracted': self.total_players_extracted,
            'elapsed_time': elapsed,
            'success_rate': len(self.successful_matches)/self.processed_matches*100 if self.processed_matches > 0 else 0,
            'processing_rate': len(self.successful_matches)/elapsed*60 if elapsed > 0 else 0,
            'failed_match_details': self.failed_matches
        }

def main():
    """Main function to run additional 144 batch extraction"""
    
    extractor = Additional144RosterExtractor()
    results = extractor.process_all_matches()
    
    # Comprehensive final verification
    logger.info(f"\n🔍 COMPREHENSIVE FINAL VERIFICATION:")
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
    
    logger.info(f"🎯 OVERALL DATABASE STATUS:")
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
    
    logger.info(f"\n📊 SEASON BREAKDOWN:")
    for season_year, match_count, player_count in season_breakdown:
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players")
    
    conn.close()
    
    return results

if __name__ == "__main__":
    results = main()