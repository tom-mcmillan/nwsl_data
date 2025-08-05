#!/usr/bin/env python3
"""
HTML Player Stats Extractor - Extract individual player performance data
Extract Player Stats from locally saved FBRef HTML files using BeautifulSoup
Following the same methodology as the team stats extractor
"""

import sqlite3
import os
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HTMLPlayerStatsExtractor:
    """
    Extract Player Stats from saved FBRef HTML files using BeautifulSoup methodology
    """
    
    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        self.processed_matches = []
        self.failed_matches = []
        
    def extract_player_stats_from_html(self, html_content: str, match_id: str) -> Optional[Dict[str, List[Dict]]]:
        """
        Extract Player Stats from HTML using BeautifulSoup
        Returns dict with home_team and away_team player lists
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Find both team player stats tables
            home_stats = self._extract_team_player_stats(soup, 'home', match_id)
            away_stats = self._extract_team_player_stats(soup, 'away', match_id)
            
            if not home_stats and not away_stats:
                logger.warning(f"⚠️  No player stats found for match {match_id}")
                return None
                
            return {
                'home_team': home_stats or [],
                'away_team': away_stats or []
            }
            
        except Exception as e:
            logger.error(f"❌ Error extracting player stats from match {match_id}: {str(e)}")
            return None
    
    def _extract_team_player_stats(self, soup: BeautifulSoup, team_side: str, match_id: str) -> Optional[List[Dict]]:
        """Extract player stats for one team from the HTML"""
        try:
            # Find player stats tables - they have IDs like "stats_[team_id]_summary"
            stats_tables = soup.find_all('table', class_='stats_table')
            
            if not stats_tables:
                logger.warning(f"⚠️  No stats tables found for match {match_id}")
                return None
            
            # Look for summary stats tables (contain goals, assists, etc.)
            summary_tables = [table for table in stats_tables if 'summary' in table.get('id', '')]
            
            if len(summary_tables) < 2:
                logger.warning(f"⚠️  Expected 2 summary tables, found {len(summary_tables)} for match {match_id}")
                return None
                
            # Determine which table is home vs away based on position or content
            table_index = 0 if team_side == 'home' else 1
            if table_index >= len(summary_tables):
                return None
                
            return self._parse_player_stats_table(summary_tables[table_index], match_id)
            
        except Exception as e:
            logger.error(f"❌ Error extracting {team_side} team player stats: {str(e)}")
            return None
    
    def _parse_player_stats_table(self, table, match_id: str) -> List[Dict]:
        """Parse individual player statistics from a stats table"""
        players = []
        
        try:
            # Get table rows
            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
            
            for row in rows:
                player_data = self._extract_player_row_data(row, match_id)
                if player_data:
                    players.append(player_data)
                    
            logger.info(f"📊 Extracted stats for {len(players)} players")
            return players
            
        except Exception as e:
            logger.error(f"❌ Error parsing player stats table: {str(e)}")
            return []
    
    def _extract_player_row_data(self, row, match_id: str) -> Optional[Dict]:
        """Extract data from a single player row"""
        try:
            cells = row.find_all(['th', 'td'])
            if len(cells) < 6:  # Minimum expected columns
                return None
            
            # Extract player basic info
            player_cell = cells[0]  # First cell contains player name and link
            player_link = player_cell.find('a')
            
            if not player_link:
                return None
                
            player_name = player_link.get_text().strip()
            player_url = player_link.get('href', '')
            
            # Extract player_id from URL (e.g., "/en/players/7824185a/Barbra-Banda" -> "7824185a")
            player_id_match = re.search(r'/players/([a-f0-9]+)/', player_url)
            player_id = player_id_match.group(1) if player_id_match else None
            
            if not player_id:
                logger.warning(f"⚠️  Could not extract player_id for {player_name}")
                return None
            
            # Extract basic stats (shirt number, nation, position, age, minutes)
            shirt_number = self._safe_extract_text(cells[1]) if len(cells) > 1 else None
            nationality = self._extract_nationality(cells[2]) if len(cells) > 2 else None
            position = self._safe_extract_text(cells[3]) if len(cells) > 3 else None
            age = self._safe_extract_text(cells[4]) if len(cells) > 4 else None
            minutes = self._safe_extract_int(cells[5]) if len(cells) > 5 else None
            
            # Extract performance stats - these are the key stats we want
            performance_stats = {}
            if len(cells) > 6:
                # Map cell positions to stat names based on typical FBRef table structure
                stat_mapping = {
                    6: 'goals',           # Gls
                    7: 'assists',         # Ast  
                    8: 'pens_made',       # PK
                    9: 'pens_att',        # PKatt
                    10: 'shots',          # Sh
                    11: 'shots_on_target', # SoT
                    12: 'cards_yellow',   # CrdY
                    13: 'cards_red',      # CrdR
                    14: 'touches',        # Touches
                    15: 'tackles',        # Tkl
                    16: 'interceptions',  # Int
                    17: 'blocks',         # Blocks
                    18: 'xg',             # xG
                    19: 'npxg',           # npxG
                    20: 'xg_assist',      # xAG
                    21: 'sca',            # SCA
                    22: 'gca',            # GCA
                    23: 'passes_completed', # Cmp
                    24: 'passes',         # Att
                    25: 'passes_pct',     # Cmp%
                    26: 'progressive_passes', # PrgP
                    27: 'carries',        # Carries
                    28: 'progressive_carries', # PrgC
                    29: 'take_ons',       # Att
                    30: 'take_ons_won'    # Succ
                }
                
                for cell_idx, stat_name in stat_mapping.items():
                    if cell_idx < len(cells):
                        if stat_name in ['xg', 'npxg', 'xg_assist', 'passes_pct']:
                            # These are float values
                            performance_stats[stat_name] = self._safe_extract_float(cells[cell_idx])
                        else:
                            # These are integer values
                            performance_stats[stat_name] = self._safe_extract_int(cells[cell_idx])
            
            return {
                'match_id': match_id,
                'player_id': player_id,
                'player_name': player_name,
                'shirt_number': self._safe_int_conversion(shirt_number),
                'nationality': nationality,
                'position': position,
                'age': age,
                'minutes': minutes,
                **performance_stats
            }
            
        except Exception as e:
            logger.error(f"❌ Error extracting player row data: {str(e)}")
            return None
    
    def _safe_extract_text(self, cell) -> Optional[str]:
        """Safely extract text from a cell"""
        try:
            text = cell.get_text().strip()
            return text if text and text != '—' and text != '' else None
        except:
            return None
    
    def _safe_extract_int(self, cell) -> Optional[int]:
        """Safely extract integer from a cell"""
        try:
            text = self._safe_extract_text(cell)
            if text and text.isdigit():
                return int(text)
            return None
        except:
            return None
    
    def _safe_extract_float(self, cell) -> Optional[float]:
        """Safely extract float from a cell"""
        try:
            text = self._safe_extract_text(cell)
            if text:
                # Handle percentage signs
                text = text.replace('%', '')
                return float(text)
            return None
        except:
            return None
    
    def _safe_int_conversion(self, value) -> Optional[int]:
        """Safely convert value to int"""
        try:
            if value and str(value).isdigit():
                return int(value)
            return None
        except:
            return None
    
    def _extract_nationality(self, cell) -> Optional[str]:
        """Extract nationality from nation cell"""
        try:
            # Look for country code in the cell
            text = cell.get_text().strip()
            # Extract 3-letter country code (e.g., "USA", "BRA", "ZAM")
            if len(text) >= 3:
                return text[-3:] if text[-3:].isalpha() else None
            return None
        except:
            return None
    
    def process_html_file(self, html_file_path: str) -> bool:
        """Process a single HTML file and extract player stats"""
        try:
            # Extract match_id from filename
            match_id = os.path.basename(html_file_path).replace('match_', '').replace('.html', '')
            
            # Read HTML content
            with open(html_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Extract player stats
            player_stats = self.extract_player_stats_from_html(html_content, match_id)
            
            if player_stats:
                total_players = len(player_stats.get('home_team', [])) + len(player_stats.get('away_team', []))
                logger.info(f"✅ Successfully extracted stats for {total_players} players in match {match_id}")
                self.processed_matches.append((match_id, player_stats))
                return True
            else:
                logger.warning(f"⚠️  Failed to extract player stats for match {match_id}")
                self.failed_matches.append(match_id)
                return False
                
        except Exception as e:
            logger.error(f"❌ Error processing file {html_file_path}: {str(e)}")
            return False
    
    def process_html_directory(self, html_dir: str) -> Dict[str, any]:
        """Process all HTML files in a directory"""
        logger.info(f"🚀 Processing HTML files for player stats in {html_dir}")
        
        html_files = [f for f in os.listdir(html_dir) if f.endswith('.html') and f.startswith('match_')]
        logger.info(f"📄 Found {len(html_files)} HTML match files")
        
        results = {
            'processed': 0,
            'failed': 0,
            'extracted_stats': []
        }
        
        for html_file in sorted(html_files):
            html_path = os.path.join(html_dir, html_file)
            if self.process_html_file(html_path):
                results['processed'] += 1
            else:
                results['failed'] += 1
        
        results['extracted_stats'] = self.processed_matches
        
        logger.info(f"📊 Player stats processing complete: {results['processed']} success, {results['failed']} failed")
        return results

# Usage functions
def extract_player_stats_from_saved_html(html_dir: str) -> Dict[str, any]:
    """
    Main function to extract Player Stats from saved HTML files
    """
    extractor = HTMLPlayerStatsExtractor()
    return extractor.process_html_directory(html_dir)

def test_single_match_players(html_file_path: str):
    """Test player extraction on a single match file"""
    extractor = HTMLPlayerStatsExtractor()
    return extractor.process_html_file(html_file_path)

logger.info("⚽ HTML Player Stats Extractor ready!")
logger.info("📖 Usage: extract_player_stats_from_saved_html('/path/to/html/files/')")