#!/usr/bin/env python3
"""
Database Inserter - Insert extracted player stats into match_player table
"""

import sqlite3
import uuid
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class DatabaseInserter:
    """Insert player stats into the match_player table"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    def insert_match_players(self, players: List[Dict]) -> bool:
        """Insert list of player dictionaries into match_player table"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if match_id already has data
            if players:
                match_id = players[0]['match_id']
                cursor.execute("SELECT COUNT(*) FROM match_player WHERE match_id = ?", (match_id,))
                existing_count = cursor.fetchone()[0]
                
                if existing_count > 0:
                    logger.info(f"⚠️  Match {match_id} already has {existing_count} player records. Skipping.")
                    conn.close()
                    return True
            
            # Insert each player
            inserted_count = 0
            for player in players:
                # Generate unique match_player_id
                player['match_player_id'] = str(uuid.uuid4())[:8]
                
                # Resolve team_id and player_id if possible
                self._resolve_team_info(cursor, player)
                
                # Insert player record
                if self._insert_player_record(cursor, player):
                    inserted_count += 1
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Successfully inserted {inserted_count}/{len(players)} player records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting player data: {str(e)}")
            return False
    
    def _resolve_team_info(self, cursor, player: Dict):
        """Resolve team_id and player_id from existing database data"""
        
        # Try to resolve team_name from team_id if we have hex team_id
        if player.get('team_id'):
            cursor.execute("SELECT team_name_1 FROM team WHERE team_id = ?", (player['team_id'],))
            result = cursor.fetchone()
            if result:
                player['team_name'] = result[0]
        
        # Try to resolve player_id from player name
        if player.get('player_name'):
            cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player['player_name'],))
            result = cursor.fetchone()
            if result:
                player['player_id'] = result[0]
        
        # Try to resolve season_id from match_id
        if player.get('match_id'):
            cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (player['match_id'],))
            result = cursor.fetchone()
            if result:
                player['season_id'] = result[0]
    
    def _insert_player_record(self, cursor, player: Dict) -> bool:
        """Insert a single player record into match_player table"""
        
        try:
            # Build INSERT statement for all schema fields
            insert_sql = """
                INSERT INTO match_player (
                    match_player_id, match_id, player_id, player_name, team_id, team_name,
                    shirt_number, position, minutes_played, season_id,
                    
                    -- Summary Performance
                    summary_perf_gls, summary_perf_ast, summary_perf_pk, summary_perf_pkatt,
                    summary_perf_sh, summary_perf_sot, summary_perf_crdy, summary_perf_crdr,
                    summary_perf_touches, summary_perf_tkl, summary_perf_int, summary_perf_blocks,
                    
                    -- Summary Expected
                    summary_exp_xg, summary_exp_npxg, summary_exp_xag,
                    
                    -- Summary SCA/GCA
                    summary_sca_sca, summary_sca_gca,
                    
                    -- Summary Passing
                    summary_pass_cmp, summary_pass_att, summary_pass_cmp_pct, summary_pass_prgp,
                    
                    -- Summary Carries & Take-ons
                    summary_carry_carries, summary_carry_prgc, summary_take_att, summary_take_succ,
                    
                    -- Passing Tab
                    passing_total_cmp, passing_total_att, passing_total_cmp_pct,
                    passing_total_totdist, passing_total_prgdist,
                    passing_short_cmp, passing_short_att, passing_short_cmp_pct,
                    passing_medium_cmp, passing_medium_att, passing_medium_cmp_pct,
                    passing_long_cmp, passing_long_att, passing_long_cmp_pct,
                    passing_ast, passing_xag, passing_xa, passing_kp,
                    passing_final_third, passing_ppa, passing_crspa, passing_prgp,
                    
                    -- Pass Types Tab
                    pass_types_att, pass_types_live, pass_types_dead, pass_types_fk,
                    pass_types_tb, pass_types_sw, pass_types_crs, pass_types_ti, pass_types_ck,
                    corner_in, corner_out, corner_str,
                    pass_outcome_cmp, pass_outcome_off, pass_outcome_blocks,
                    
                    -- Defensive Actions Tab
                    def_tkl, def_tklw, def_tkl_def_3rd, def_tkl_mid_3rd, def_tkl_att_3rd,
                    def_chal_tkl, def_chal_att, def_chal_tkl_pct, def_chal_lost,
                    def_blocks_total, def_blocks_sh, def_blocks_pass,
                    def_int, def_tkl_int, def_clr, def_err,
                    
                    -- Possession Tab
                    poss_touches, poss_touches_def_pen, poss_touches_def_3rd,
                    poss_touches_mid_3rd, poss_touches_att_3rd, poss_touches_att_pen, poss_touches_live,
                    poss_take_att, poss_take_succ, poss_take_succ_pct, poss_take_tkld, poss_take_tkld_pct,
                    poss_carry_carries, poss_carry_totdist, poss_carry_prgdist, poss_carry_prgc,
                    poss_carry_final_third, poss_carry_cpa, poss_carry_mis, poss_carry_dis,
                    poss_rec_rec, poss_rec_prgr,
                    
                    -- Misc Stats Tab
                    misc_crdy, misc_crdr, misc_2crdy, misc_fls, misc_fld, misc_off,
                    misc_crs, misc_int, misc_tklw, misc_pkwon, misc_pkcon, misc_og, misc_recov,
                    
                    -- Aerial Duels
                    aerial_won, aerial_lost, aerial_won_pct
                ) VALUES ({})
            """.format(','.join(['?' for _ in range(132)]))  # 132 fields total
            
            # Create values tuple in same order as INSERT
            values = (
                player.get('match_player_id'),
                player.get('match_id'),
                player.get('player_id'),
                player.get('player_name'),
                player.get('team_id'),
                player.get('team_name'),
                player.get('shirt_number'),
                player.get('position'),
                player.get('minutes_played'),
                player.get('season_id'),
                
                # Summary Performance
                player.get('summary_perf_gls'),
                player.get('summary_perf_ast'),
                player.get('summary_perf_pk'),
                player.get('summary_perf_pkatt'),
                player.get('summary_perf_sh'),
                player.get('summary_perf_sot'),
                player.get('summary_perf_crdy'),
                player.get('summary_perf_crdr'),
                player.get('summary_perf_touches'),
                player.get('summary_perf_tkl'),
                player.get('summary_perf_int'),
                player.get('summary_perf_blocks'),
                
                # Summary Expected
                player.get('summary_exp_xg'),
                player.get('summary_exp_npxg'),
                player.get('summary_exp_xag'),
                
                # Summary SCA/GCA
                player.get('summary_sca_sca'),
                player.get('summary_sca_gca'),
                
                # Summary Passing
                player.get('summary_pass_cmp'),
                player.get('summary_pass_att'),
                player.get('summary_pass_cmp_pct'),
                player.get('summary_pass_prgp'),
                
                # Summary Carries & Take-ons
                player.get('summary_carry_carries'),
                player.get('summary_carry_prgc'),
                player.get('summary_take_att'),
                player.get('summary_take_succ'),
                
                # Passing Tab - set all to None for now (will enhance later)
                *[player.get(f'passing_{field}') for field in [
                    'total_cmp', 'total_att', 'total_cmp_pct', 'total_totdist', 'total_prgdist',
                    'short_cmp', 'short_att', 'short_cmp_pct',
                    'medium_cmp', 'medium_att', 'medium_cmp_pct',
                    'long_cmp', 'long_att', 'long_cmp_pct',
                    'ast', 'xag', 'xa', 'kp', 'final_third', 'ppa', 'crspa', 'prgp'
                ]],
                
                # Pass Types Tab - set all to None for now
                *[player.get(f'pass_types_{field}') for field in [
                    'att', 'live', 'dead', 'fk', 'tb', 'sw', 'crs', 'ti', 'ck'
                ]],
                *[player.get(f'corner_{field}') for field in ['in', 'out', 'str']],
                *[player.get(f'pass_outcome_{field}') for field in ['cmp', 'off', 'blocks']],
                
                # Defensive Actions Tab
                *[player.get(f'def_{field}') for field in [
                    'tkl', 'tklw', 'tkl_def_3rd', 'tkl_mid_3rd', 'tkl_att_3rd',
                    'chal_tkl', 'chal_att', 'chal_tkl_pct', 'chal_lost',
                    'blocks_total', 'blocks_sh', 'blocks_pass',
                    'int', 'tkl_int', 'clr', 'err'
                ]],
                
                # Possession Tab
                *[player.get(f'poss_{field}') for field in [
                    'touches', 'touches_def_pen', 'touches_def_3rd', 'touches_mid_3rd',
                    'touches_att_3rd', 'touches_att_pen', 'touches_live',
                    'take_att', 'take_succ', 'take_succ_pct', 'take_tkld', 'take_tkld_pct',
                    'carry_carries', 'carry_totdist', 'carry_prgdist', 'carry_prgc',
                    'carry_final_third', 'carry_cpa', 'carry_mis', 'carry_dis',
                    'rec_rec', 'rec_prgr'
                ]],
                
                # Misc Stats Tab
                *[player.get(f'misc_{field}') for field in [
                    'crdy', 'crdr', '2crdy', 'fls', 'fld', 'off',
                    'crs', 'int', 'tklw', 'pkwon', 'pkcon', 'og', 'recov'
                ]],
                
                # Aerial Duels
                player.get('aerial_won'),
                player.get('aerial_lost'),
                player.get('aerial_won_pct')
            )
            
            cursor.execute(insert_sql, values)
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting player {player.get('player_name', 'Unknown')}: {str(e)}")
            return False

def test_database_insertion():
    """Test database insertion with sample data"""
    
    # Create sample player data
    sample_players = [
        {
            'match_id': 'test123',
            'player_name': 'Test Player 1',
            'team_id': 'test_team',
            'summary_perf_gls': 1,
            'summary_perf_ast': 0,
            'summary_exp_xg': 0.5,
        },
        {
            'match_id': 'test123',
            'player_name': 'Test Player 2', 
            'team_id': 'test_team',
            'summary_perf_gls': 0,
            'summary_perf_ast': 1,
            'summary_exp_xg': 0.2,
        }
    ]
    
    db_path = "data/processed/nwsldata.db"
    inserter = DatabaseInserter(db_path)
    
    success = inserter.insert_match_players(sample_players)
    print(f"Insertion test: {'SUCCESS' if success else 'FAILED'}")

if __name__ == "__main__":
    test_database_insertion()