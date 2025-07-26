#!/usr/bin/env python3
"""
Match ID Fixer
Generates proper match_ids for NULL entries in match table
"""

import sqlite3
import hashlib
import uuid

class MatchIdFixer:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def fix_null_match_ids(self):
        """Generate match_ids for rows with NULL/empty match_ids"""
        print("🔧 Starting match_id fixing...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get matches with null/empty match_ids
        null_matches = conn.execute('''
            SELECT rowid, season_id, match_date, home_team_id, away_team_id, filename
            FROM match 
            WHERE match_id IS NULL OR match_id = ''
            ORDER BY match_date DESC
        ''').fetchall()
        
        print(f"📊 Found {len(null_matches)} matches with NULL/empty match_ids")
        
        updated_count = 0
        for rowid, season_id, match_date, home_team_id, away_team_id, filename in null_matches:
            try:
                # Generate a unique match_id based on key data
                # Use combination of date, home_team, away_team to create consistent ID
                id_string = f"{match_date}:{home_team_id}:{away_team_id}"
                match_id = hashlib.md5(id_string.encode()).hexdigest()[:8]
                
                # Update the match record
                conn.execute('''
                    UPDATE match 
                    SET match_id = ? 
                    WHERE rowid = ?
                ''', (match_id, rowid))
                
                updated_count += 1
                
                # Get team names for display
                home_team_name = conn.execute('SELECT team_name FROM teams WHERE team_id = ?', (home_team_id,)).fetchone()
                away_team_name = conn.execute('SELECT team_name FROM teams WHERE team_id = ?', (away_team_id,)).fetchone()
                
                home_name = home_team_name[0] if home_team_name else home_team_id
                away_name = away_team_name[0] if away_team_name else away_team_id
                
                print(f"  📝 Generated {match_id} for {home_name} vs {away_name} ({match_date})")
                
            except Exception as e:
                print(f"⚠️ Error generating match_id for rowid {rowid}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 Match ID fixing complete!")
        print(f"   ✅ Generated {updated_count} new match_ids")
        
        return updated_count

if __name__ == "__main__":
    fixer = MatchIdFixer()
    fixer.fix_null_match_ids()