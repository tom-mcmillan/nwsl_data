#!/usr/bin/env python3
"""
Extract 2022 statistics for Stefany Ferrer and Vanessa Gilles from HTML files
"""

import sqlite3
import os
from fbref_player_extractor import FBRefPlayerExtractor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Player2022Extractor:
    def __init__(self):
        self.db_path = "data/processed/nwsldata.db"
        self.html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
        self.extractor = FBRefPlayerExtractor(self.db_path)
        
        # Target players
        self.target_players = {
            "Stefany Ferrer": {
                "player_id": "13f871ac",
                "fbref_id": "13f871ac",
                "matches": []
            },
            "Vanessa Gilles": {
                "player_id": "a179cd70", 
                "fbref_id": "a179cd70",
                "matches": []
            }
        }
    
    def get_player_matches(self):
        """Get 2022 matches for both target players"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for player_name, player_data in self.target_players.items():
            cursor.execute("""
                SELECT mps.match_player_summary_id, m.match_id, m.match_date, mps.minutes_played
                FROM match_player_summary mps 
                JOIN match m ON mps.match_id = m.match_id 
                WHERE mps.player_id = ? AND mps.season_id = '2022'
                ORDER BY m.match_date
            """, (player_data["player_id"],))
            
            matches = cursor.fetchall()
            player_data["matches"] = matches
            logger.info(f"Found {len(matches)} matches for {player_name} in 2022")
            
        conn.close()
    
    def extract_player_stats_from_html(self, match_id, player_name):
        """Extract stats for a specific player from HTML file"""
        html_file = os.path.join(self.html_dir, f"match_{match_id}.html")
        
        if not os.path.exists(html_file):
            logger.warning(f"HTML file not found for match {match_id}")
            return None
        
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Extract all players from this match
            all_players = self.extractor.extract_match_player_stats(html_content, match_id)
            
            if not all_players:
                logger.warning(f"No players extracted from match {match_id}")
                return None
            
            # Find our target player
            for player_data in all_players:
                if player_data['player_name'] == player_name:
                    logger.info(f"✅ Found {player_name} in match {match_id}")
                    return player_data
            
            logger.warning(f"❌ {player_name} not found in match {match_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error processing match {match_id}: {str(e)}")
            return None
    
    def update_match_player_summary(self, match_player_summary_id, stats):
        """Update match_player_summary record with extracted stats"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Map extracted stats to database columns
        update_fields = []
        values = []
        
        stat_mappings = {
            'summary_perf_gls': 'goals',
            'summary_perf_ast': 'assists', 
            'summary_perf_pk': 'penalty_kicks',
            'summary_perf_pkatt': 'penalty_kicks_attempted',
            'summary_perf_sh': 'shots',
            'summary_perf_sot': 'shots_on_target',
            'summary_perf_crdy': 'yellow_cards',
            'summary_perf_crdr': 'red_cards',
            'summary_perf_touches': 'touches',
            'summary_perf_tkl': 'tackles',
            'summary_perf_int': 'interceptions',
            'summary_perf_blocks': 'blocks',
            'summary_exp_xg': 'xg',
            'summary_exp_npxg': 'npxg',
            'summary_exp_xag': 'xag',
            'summary_sca_sca': 'sca',
            'summary_sca_gca': 'gca',
            'summary_pass_cmp': 'passes_completed',
            'summary_pass_att': 'passes_attempted',
            'summary_pass_cmp_pct': 'pass_completion_pct',
            'summary_pass_prgp': 'progressive_passes',
            'summary_carry_carries': 'carries',
            'summary_carry_prgc': 'progressive_carries',
            'summary_take_att': 'take_ons_attempted',
            'summary_take_succ': 'take_ons_successful'
        }
        
        for stat_key, db_column in stat_mappings.items():
            if stat_key in stats and stats[stat_key] is not None:
                update_fields.append(f"{db_column} = ?")
                values.append(stats[stat_key])
        
        if update_fields:
            query = f"""
                UPDATE match_player_summary 
                SET {', '.join(update_fields)}
                WHERE match_player_summary_id = ?
            """
            values.append(match_player_summary_id)
            
            cursor.execute(query, values)
            conn.commit()
            logger.info(f"✅ Updated {len(update_fields)} stats for record {match_player_summary_id}")
        else:
            logger.warning(f"⚠️ No stats to update for record {match_player_summary_id}")
        
        conn.close()
    
    def process_all_matches(self):
        """Process all 2022 matches for both players"""
        self.get_player_matches()
        
        total_updated = 0
        total_failed = 0
        
        for player_name, player_data in self.target_players.items():
            logger.info(f"\n🔍 Processing {player_name} ({len(player_data['matches'])} matches)")
            
            for match_record in player_data['matches']:
                match_player_summary_id, match_id, match_date, minutes = match_record
                
                logger.info(f"Processing match {match_id} ({match_date}) - {minutes} minutes")
                
                # Extract stats from HTML
                stats = self.extract_player_stats_from_html(match_id, player_name)
                
                if stats:
                    # Update database record
                    self.update_match_player_summary(match_player_summary_id, stats)
                    total_updated += 1
                else:
                    total_failed += 1
                    logger.error(f"❌ Failed to extract stats for {player_name} in match {match_id}")
        
        logger.info(f"\n📊 SUMMARY:")
        logger.info(f"✅ Successfully updated: {total_updated} records")
        logger.info(f"❌ Failed to update: {total_failed} records")
        
        return total_updated, total_failed

def main():
    processor = Player2022Extractor()
    updated, failed = processor.process_all_matches()
    
    if updated > 0:
        print(f"\n🎉 Successfully extracted and updated statistics for {updated} match records!")
    if failed > 0:
        print(f"⚠️ Failed to process {failed} match records")

if __name__ == "__main__":
    main()