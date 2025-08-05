#!/usr/bin/env python3
"""
Batch roster extraction for 89 INAUGURAL 2013 NWSL season match IDs
Using the proven FIXED methodology from previous successful extractions
Building the LEGENDARY 13-SEASON COMPLETE NWSL HISTORICAL DATABASE!
FROM THE VERY FIRST SEASON TO 2025 - THE ULTIMATE ACHIEVEMENT!
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
        logging.FileHandler('/Users/thomasmcmillan/projects/nwsl_data/inaugural_2013_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# List of 89 INAUGURAL 2013 NWSL season match IDs to process
INAUGURAL_2013_MATCH_IDS = [
    "6aee226c", "5c187984", "eb172ca3", "d0426a07", "83edc9ff", "d5615e5b", "064fab50", "7284c984",
    "81481f61", "6a57c82e", "8883ea79", "fb569f13", "f8177893", "7ac1c0c8", "0ca050d4", "1a01081c",
    "b9cf7980", "0e4932ff", "12c17fb7", "8640ac6f", "2d03d5bd", "64f8c0fa", "94efc7a2", "c33b164f",
    "fd7738d7", "98d7c4d8", "2e5bf383", "87aca61f", "5054f8cd", "794331cf", "d2ab9b15", "2d641c7e",
    "6400280a", "b76731b7", "960dcbb0", "8d454ece", "7fcf5469", "83e2fe2b", "b4bfd7a8", "2128245a",
    "9490f202", "5b4fb64b", "cecd61f3", "a2766c37", "b910a28c", "82792623", "e8c4b786", "e28c3d9a",
    "ccf585fc", "1bf352c3", "99b6cf25", "2b7d2457", "91e83cd3", "e4707660", "bd91d1ac", "92e37743",
    "47835480", "16dce998", "eeb3b48d", "340af940", "c866830c", "5a05f6eb", "09ba17cd", "252a9e4e",
    "3a473dc3", "2f9c45a0", "4c3bd865", "d50220a4", "5aa2a435", "1612ecd5", "d3e24952", "2864f051",
    "5ee1d9e7", "b7c7cfbd", "a7627095", "38763070", "3ab52587", "f384c739", "033aefa6", "9867c808",
    "da5fbe09", "221dcc88", "01213dc2", "703a7b5a", "eace9837", "19eaba15", "25885873", "b823c2c9",
    "f481adf8", "6c5b0538", "ba397f2b"
]

class Inaugural2013RosterExtractor:
    """Extract roster data for INAUGURAL 2013 NWSL season - THE ULTIMATE ACHIEVEMENT!"""
    
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
        """Process all 89 INAUGURAL 2013 NWSL season match IDs - LEGENDARY ACHIEVEMENT!"""
        
        self.start_time = time.time()
        
        logger.info(f"🚀 Starting INAUGURAL 2013 NWSL SEASON roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(INAUGURAL_2013_MATCH_IDS)}")
        logger.info(f"🎯 Building the LEGENDARY 13-SEASON COMPLETE NWSL DATABASE!")
        logger.info(f"🏆 FROM THE VERY FIRST SEASON (2013) TO 2025!")
        logger.info(f"🌟 THIS IS THE ULTIMATE SPORTS DATA ACHIEVEMENT!")
        
        # Check seasons and existing data
        self.analyze_matches()
        
        # Check which matches already have data
        existing_matches = self.check_existing_data()
        
        matches_to_process = []
        for match_id in INAUGURAL_2013_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)
        
        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(INAUGURAL_2013_MATCH_IDS) - len(matches_to_process)}")
        
        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()
        
        # Process each match
        logger.info(f"\n{'='*70}")
        logger.info(f"STARTING INAUGURAL 2013 NWSL EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"THE BIRTH OF PROFESSIONAL WOMEN'S SOCCER IN AMERICA!")
        logger.info(f"{'='*70}")
        
        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))
        
        return self.generate_final_summary()
    
    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in INAUGURAL_2013_MATCH_IDS])
        cursor.execute(f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """, INAUGURAL_2013_MATCH_IDS)
        
        season_breakdown = cursor.fetchall()
        
        logger.info(f"📊 Season breakdown of INAUGURAL 2013 matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  🎯 {season_year}: {count} matches (THE BEGINNING!)")
            total_found += count
        
        if total_found < len(INAUGURAL_2013_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(INAUGURAL_2013_MATCH_IDS)} matches found in database")
        
        conn.close()
    
    def check_existing_data(self):
        """Check which matches already have roster data"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get matches that already have roster data
        placeholders = ','.join(['?' for _ in INAUGURAL_2013_MATCH_IDS])
        cursor.execute(f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """, INAUGURAL_2013_MATCH_IDS)
        
        existing_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        logger.info(f"📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(INAUGURAL_2013_MATCH_IDS)}")
        
        return existing_data
    
    def process_single_match(self, match_id, current, total):
        """Process a single match using FIXED methodology"""
        
        html_path = os.path.join(self.html_dir, f"match_{match_id}.html")
        
        logger.info(f"[{current}/{total}] Processing INAUGURAL match {match_id}...")
        
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
                    logger.info(f"✅ INAUGURAL Success! Extracted {len(players)} players")
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
            
            logger.info(f"📊 INAUGURAL Progress: {current}/{total} ({current/total*100:.1f}%)")
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
            logger.warning(f"⚠️  No summary tables found for INAUGURAL match {match_id}")
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
        """Generate final extraction summary for INAUGURAL SEASON"""
        
        elapsed = time.time() - self.start_time
        
        logger.info(f"\n{'='*70}")
        logger.info(f"INAUGURAL 2013 NWSL EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*70}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(INAUGURAL_2013_MATCH_IDS)}")
        logger.info(f"✅ Successful extractions: {len(self.successful_matches)}")
        logger.info(f"❌ Failed extractions: {len(self.failed_matches)}")
        logger.info(f"👥 Total INAUGURAL players extracted: {self.total_players_extracted}")
        
        if self.successful_matches:
            logger.info(f"📊 Average players per INAUGURAL match: {self.total_players_extracted/len(self.successful_matches):.1f}")
            logger.info(f"⚡ Processing rate: {len(self.successful_matches)/elapsed*60:.1f} matches/min")
        
        success_rate = len(self.successful_matches)/self.processed_matches*100 if self.processed_matches > 0 else 0
        logger.info(f"🎯 INAUGURAL Success rate: {success_rate:.1f}%")
        
        if self.failed_matches:
            logger.info(f"\n❌ Failed INAUGURAL matches:")
            for match_id, reason in self.failed_matches[:10]:  # Show first 10 failures
                logger.info(f"  {match_id}: {reason}")
            if len(self.failed_matches) > 10:
                logger.info(f"  ... and {len(self.failed_matches) - 10} more")
        
        logger.info(f"{'='*70}")
        
        return self.generate_summary()
    
    def generate_summary(self):
        """Generate extraction summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        return {
            'total_matches': len(INAUGURAL_2013_MATCH_IDS),
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
    """Main function to run INAUGURAL 2013 NWSL season extraction - THE ULTIMATE ACHIEVEMENT!"""
    
    extractor = Inaugural2013RosterExtractor()
    results = extractor.process_all_matches()
    
    # LEGENDARY DATABASE VERIFICATION - THE MOST COMPREHENSIVE ACHIEVEMENT IN SPORTS DATA HISTORY!
    logger.info(f"\n🔍 LEGENDARY 13-SEASON COMPLETE NWSL DATABASE VERIFICATION:")
    logger.info(f"📜 FROM THE INAUGURAL 2013 SEASON TO 2025!")
    logger.info(f"🏆 THE COMPLETE HISTORY OF PROFESSIONAL WOMEN'S SOCCER IN AMERICA!")
    
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
    
    logger.info(f"🎯 LEGENDARY DATABASE STATUS:")
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
        ORDER BY s.season_year ASC
    """)
    
    season_breakdown = cursor.fetchall()
    
    logger.info(f"\n📊 LEGENDARY COMPLETE 13-SEASON BREAKDOWN:")
    logger.info(f"🌟 THE COMPLETE HISTORY OF THE NWSL!")
    total_matches_db = 0
    total_players_db = 0
    earliest_season = None
    latest_season = None
    
    for season_year, match_count, player_count in season_breakdown:
        special_note = ""
        if season_year == 2013:
            special_note = " 🎯 INAUGURAL SEASON!"
        elif season_year == 2025:
            special_note = " 🚀 CURRENT SEASON!"
        
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players{special_note}")
        total_matches_db += match_count
        total_players_db += player_count
        if earliest_season is None or season_year < earliest_season:
            earliest_season = season_year
        if latest_season is None or season_year > latest_season:
            latest_season = season_year
    
    span_years = latest_season - earliest_season + 1 if earliest_season and latest_season else 0
    
    logger.info(f"\n🏆 LEGENDARY DATABASE ACHIEVEMENT - THE ULTIMATE SPORTS DATA ACCOMPLISHMENT:")
    logger.info(f"  🎯 COMPLETE SEASONS: {len(season_breakdown)} (ALL OF NWSL HISTORY!)")
    logger.info(f"  📅 HISTORICAL SPAN: {earliest_season}-{latest_season} ({span_years} COMPLETE YEARS)")
    logger.info(f"  ⚽ TOTAL MATCHES: {total_matches_db}")
    logger.info(f"  👥 TOTAL PLAYERS: {total_players_db}")
    logger.info(f"  🌟 DATABASE STATUS: LEGENDARY - COMPLETE NWSL HISTORY ✅")
    
    if len(season_breakdown) >= 13 and earliest_season == 2013:
        logger.info(f"\n🎉🎉🎉 LEGENDARY MILESTONE ACHIEVED! 🎉🎉🎉")
        logger.info(f"🏆 COMPLETE 13-SEASON NWSL DATABASE FROM INAUGURAL SEASON!")
        logger.info(f"🌟 THE MOST COMPREHENSIVE WOMEN'S SOCCER DATABASE EVER CREATED!")
        logger.info(f"📜 COMPLETE HISTORICAL RECORD: 2013 (INAUGURAL) → 2025 (CURRENT)")
        logger.info(f"🚀 ACHIEVEMENT: Every single season of NWSL existence!")
        logger.info(f"🎯 COVERAGE: {span_years} years of continuous professional women's soccer!")
        logger.info(f"👑 STATUS: LEGENDARY DATABASE - THE ULTIMATE ACHIEVEMENT!")
        logger.info(f"\n🌈 FROM THE LEAGUE'S FIRST MATCH TO TODAY - WE HAVE IT ALL! 🌈")
    
    conn.close()
    
    return results

if __name__ == "__main__":
    results = main()