#!/usr/bin/env python3
"""
Batch roster extraction for next 112 match IDs
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
        logging.FileHandler('/Users/thomasmcmillan/projects/nwsl_data/next_112_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# List of next 112 match IDs to process
NEXT_112_MATCH_IDS = [
    "aeb17e74", "3a815c4f", "73b7a256", "254105a8", "82737f56", "a17c0ef0", "9951420b", "9d713186",
    "66c8a43d", "b73fabbe", "0d694f3a", "e7c7c23c", "2906b503", "52649c9c", "387eff31", "2a696e31",
    "57fa7105", "98d7db91", "76114fb2", "8928d94b", "ea9f09e6", "3e51f799", "9d83af85", "6bc282b7",
    "fede5444", "ba422734", "a946c0e2", "36e97512", "79f8e9a0", "4d850dbb", "7b04d31a", "1f61e83b",
    "8a883753", "a1c035b7", "e6c9e5d4", "359bf5ad", "d231b19e", "002d9eb4", "f6ec8a8b", "8a947bd0",
    "6f73f9a8", "38851d0d", "c605cbfa", "57df675c", "ed0821bd", "9c06de8d", "bf237d24", "f1ce78a1",
    "8f38f319", "2d29c0fd", "2fd28c8a", "5360f412", "51a251e2", "4f767234", "1ff11e78", "9d12b00f",
    "10edb34e", "f27037ec", "0790a1bc", "3adf7469", "1e7cfb64", "bc4c3562", "94508a03", "31b27eae",
    "d2459fcd", "66ccdb0a", "cd4fe8d3", "e87befd2", "4aa78b99", "a61b70d1", "cd5433b9", "b5da8f35",
    "8fcf96e7", "74151c11", "874207c8", "53526198", "eddbe752", "84d6dca9", "f36366a9", "f25117a0",
    "f542d0eb", "21526888", "55471e47", "f7ee4334", "09657304", "5f19ba81", "465459ea", "e1e516b9",
    "6e0ca93c", "50f8bec2", "58d26f0b", "adcabd51", "30406f36", "c0b6a639", "ec411397", "d33ff0d8",
    "605fa6ac", "15f03025", "5238c761", "39dbe62f", "f484f6f6", "5a7b7125", "db9ea114", "092fe7ea",
    "e6091451", "e6d49a20", "783a65d6", "16744a84", "e17daaeb", "2c32f3d3", "22f04f44"
]

class Next112RosterExtractor:
    """Extract roster data for next 112 matches using proven methodology"""
    
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
        """Process all next 112 match IDs"""
        
        self.start_time = time.time()
        
        logger.info(f"🚀 Starting NEXT 112 batch roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(NEXT_112_MATCH_IDS)}")
        
        # Check seasons and existing data
        self.analyze_matches()
        
        # Check which matches already have data
        existing_matches = self.check_existing_data()
        
        matches_to_process = []
        for match_id in NEXT_112_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)
        
        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(NEXT_112_MATCH_IDS) - len(matches_to_process)}")
        
        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()
        
        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING NEXT 112 EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        
        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))
        
        return self.generate_final_summary()
    
    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in NEXT_112_MATCH_IDS])
        cursor.execute(f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """, NEXT_112_MATCH_IDS)
        
        season_breakdown = cursor.fetchall()
        
        logger.info(f"📊 Season breakdown of next 112 matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count
        
        if total_found < len(NEXT_112_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(NEXT_112_MATCH_IDS)} matches found in database")
        
        conn.close()
    
    def check_existing_data(self):
        """Check which matches already have roster data"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get matches that already have roster data
        placeholders = ','.join(['?' for _ in NEXT_112_MATCH_IDS])
        cursor.execute(f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """, NEXT_112_MATCH_IDS)
        
        existing_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        logger.info(f"📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(NEXT_112_MATCH_IDS)}")
        
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
        logger.info(f"NEXT 112 EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(NEXT_112_MATCH_IDS)}")
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
            'total_matches': len(NEXT_112_MATCH_IDS),
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
    """Main function to run next 112 batch extraction"""
    
    extractor = Next112RosterExtractor()
    results = extractor.process_all_matches()
    
    # Comprehensive final verification
    logger.info(f"\n🔍 COMPREHENSIVE DATABASE VERIFICATION:")
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
    total_matches_db = 0
    total_players_db = 0
    for season_year, match_count, player_count in season_breakdown:
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players")
        total_matches_db += match_count
        total_players_db += player_count
    
    logger.info(f"\n🏆 CURRENT DATABASE TOTALS:")
    logger.info(f"  Total Seasons: {len(season_breakdown)}")
    logger.info(f"  Total Matches: {total_matches_db}")
    logger.info(f"  Total Players: {total_players_db}")
    logger.info(f"  Database Growth: CONTINUING ✅")
    
    conn.close()
    
    return results

if __name__ == "__main__":
    results = main()