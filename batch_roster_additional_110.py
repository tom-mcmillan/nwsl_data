#!/usr/bin/env python3
"""
Batch roster extraction for additional 110 match IDs
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
        logging.FileHandler('/Users/thomasmcmillan/projects/nwsl_data/additional_110_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# List of additional 110 match IDs to process
ADDITIONAL_110_MATCH_IDS = [
    "afcd583a", "67a57f59", "c9ec6863", "25004f7e", "9bf95ec5", "70b9c1b6", "87083ab3", "b66598e4",
    "ff7f188e", "8780d6a5", "b87d86b7", "ae49ad75", "85ba8579", "af3f157d", "5394bc1b", "efddeda8",
    "36c46e0d", "d53451cf", "86b6cc0a", "9a2b26a1", "23114faa", "37be7787", "d26ed7f0", "7f398f88",
    "cc8ebaaa", "0f1cb3d1", "4120af97", "c79ee9c4", "622f898e", "f8c8aea4", "6420bec8", "610a4c17",
    "1cef5979", "888d23a0", "bde3da3d", "f7b69a29", "6f3dc675", "1e61252e", "eb81709b", "ec9ceb9f",
    "7ee309e3", "05482155", "b30b11e9", "745bedf3", "49094e3a", "1520b6f5", "0262bb35", "b2b9405a",
    "51020dea", "8f3fbf96", "78fac894", "efcbf7b7", "2b75137d", "dbbdb47c", "4f5b874c", "e133d584",
    "6c56c1c8", "38048580", "125df7bb", "01cdf2c9", "f7ea6cf4", "3e2273da", "a0c570ff", "3903be81",
    "453b20ed", "3690a734", "073975b1", "6d4a68e6", "e4dad184", "202faad2", "67076783", "4aa7a9c5",
    "6536d5aa", "a6063bfa", "976b8d77", "e554a812", "a0d14941", "ef3f22f7", "3b62060e", "6f44cb0a",
    "2f8d4701", "3e58ee5e", "dd37453e", "6e16e67b", "b31185f6", "aa5085c0", "4d435ae3", "fdd56674",
    "8b7900bd", "508e1cf0", "1173feeb", "d6124086", "03f02a2d", "fba4e358", "1b8fd283", "2c25dcc1",
    "96746e28", "3653dfaf", "f33364ee", "4a9bc623", "d7245076", "731bfd8e", "f2a8492d", "a6299d40",
    "b3dca21a", "840ceaac", "3ccbf5a1", "9e58d38e", "6b7e06cd", "51f18293", "9f7344bb"
]

class Additional110RosterExtractor:
    """Extract roster data for additional 110 matches using proven methodology"""
    
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
        """Process all additional 110 match IDs"""
        
        self.start_time = time.time()
        
        logger.info(f"🚀 Starting ADDITIONAL 110 batch roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(ADDITIONAL_110_MATCH_IDS)}")
        
        # Check seasons and existing data
        self.analyze_matches()
        
        # Check which matches already have data
        existing_matches = self.check_existing_data()
        
        matches_to_process = []
        for match_id in ADDITIONAL_110_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)
        
        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(ADDITIONAL_110_MATCH_IDS) - len(matches_to_process)}")
        
        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()
        
        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING ADDITIONAL 110 EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        
        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))
        
        return self.generate_final_summary()
    
    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in ADDITIONAL_110_MATCH_IDS])
        cursor.execute(f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """, ADDITIONAL_110_MATCH_IDS)
        
        season_breakdown = cursor.fetchall()
        
        logger.info(f"📊 Season breakdown of additional 110 matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count
        
        if total_found < len(ADDITIONAL_110_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(ADDITIONAL_110_MATCH_IDS)} matches found in database")
        
        conn.close()
    
    def check_existing_data(self):
        """Check which matches already have roster data"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get matches that already have roster data
        placeholders = ','.join(['?' for _ in ADDITIONAL_110_MATCH_IDS])
        cursor.execute(f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """, ADDITIONAL_110_MATCH_IDS)
        
        existing_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        logger.info(f"📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(ADDITIONAL_110_MATCH_IDS)}")
        
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
        logger.info(f"ADDITIONAL 110 EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(ADDITIONAL_110_MATCH_IDS)}")
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
            'total_matches': len(ADDITIONAL_110_MATCH_IDS),
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
    """Main function to run additional 110 batch extraction"""
    
    extractor = Additional110RosterExtractor()
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
    
    logger.info(f"🎯 UPDATED DATABASE STATUS:")
    logger.info(f"Seasons with roster data: {seasons}")
    logger.info(f"Total matches with roster data: {matches}")
    logger.info(f"Total players in database: {total_players}")
    logger.info(f"Players with shirt numbers: {with_shirts} ({with_shirts/total_players*100:.1f}%)")
    logger.info(f"Players with minutes: {with_minutes} ({with_minutes/total_players*100:.1f}%)")
    logger.info(f"Players with ID links: {with_links} ({with_links/total_players*100:.1f}%)")
    logger.info(f"Players with season_id: {with_season} ({with_season/total_players*100:.1f}%)")
    
    # Get updated season breakdown
    cursor.execute("""
        SELECT s.season_year, COUNT(DISTINCT mp.match_id) as matches, COUNT(*) as players
        FROM match_player mp 
        JOIN match m ON mp.match_id = m.match_id 
        JOIN season s ON m.season_id = s.season_id 
        GROUP BY s.season_year 
        ORDER BY s.season_year DESC
    """)
    
    season_breakdown = cursor.fetchall()
    
    logger.info(f"\n📊 UPDATED SEASON BREAKDOWN:")
    for season_year, match_count, player_count in season_breakdown:
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players")
    
    conn.close()
    
    return results

if __name__ == "__main__":
    results = main()