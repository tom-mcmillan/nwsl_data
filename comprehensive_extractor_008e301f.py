#!/usr/bin/env python3
"""
Comprehensive extractor for match 008e301f - Extract from ALL category tables
Maps to all 126 fields in match_player table
"""

import sqlite3
import os
import sys
import uuid
from bs4 import BeautifulSoup
import pandas as pd
import re

def extract_comprehensive_008e301f():
    """Extract from ALL FBRef category tables for match 008e301f"""
    
    match_id = "008e301f"
    html_path = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_008e301f.html"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    print(f"🎯 COMPREHENSIVE extraction for match {match_id}")
    print(f"📊 Target: All 126 fields in match_player table")
    
    # Read HTML
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Find all available tables
    all_tables = soup.find_all("table")
    table_ids = [table.get("id") for table in all_tables if table.get("id")]
    
    # Extract team IDs
    team_ids = []
    for tid in table_ids:
        if 'stats_' in tid and '_summary' in tid:
            team_id = tid.split('_')[1]
            team_ids.append(team_id)
    
    print(f"🏈 Found {len(team_ids)} teams: {team_ids}")
    
    # Categories to extract
    categories = ['summary', 'passing', 'passing_types', 'defense', 'possession', 'misc']
    
    all_players = {}  # Dictionary to merge data by player name
    
    # Process each team and category
    for team_id in team_ids:
        print(f"\n🔍 Processing team: {team_id}")
        
        for category in categories:
            table_id = f"stats_{team_id}_{category}"
            table = soup.find('table', id=table_id)
            
            if table:
                print(f"  ✅ Found {category} table")
                players_data = extract_category_data(table, category, team_id, match_id)
                
                # Merge into all_players
                for player_name, data in players_data.items():
                    if player_name not in all_players:
                        all_players[player_name] = data
                    else:
                        all_players[player_name].update(data)
                        
            else:
                print(f"  ❌ Missing {category} table")
    
    print(f"\n📊 EXTRACTION SUMMARY:")
    print(f"Total players: {len(all_players)}")
    
    # Show field coverage for sample player
    if all_players:
        sample_name = list(all_players.keys())[0]
        sample_player = all_players[sample_name]
        populated_fields = {k: v for k, v in sample_player.items() if v is not None}
        print(f"Sample player: {sample_name}")
        print(f"Populated fields: {len(populated_fields)}/126")
        
        # Show categories of populated fields
        categories_found = set()
        for field in populated_fields.keys():
            if 'summary_' in field:
                categories_found.add('summary')
            elif 'passing_' in field:
                categories_found.add('passing')
            elif 'def_' in field:
                categories_found.add('defense')
            elif 'poss_' in field:
                categories_found.add('possession')
            elif 'misc_' in field:
                categories_found.add('misc')
        print(f"Categories populated: {sorted(categories_found)}")
    
    # Insert into database
    print(f"\n💾 Inserting comprehensive data...")
    insert_comprehensive_data(list(all_players.values()), db_path)
    
    # Verify results
    verify_comprehensive_results(match_id, db_path)

def extract_category_data(table, category, team_id, match_id):
    """Extract data from a specific category table"""
    
    players_data = {}
    
    try:
        # Convert to DataFrame
        df = pd.read_html(str(table))[0]
        
        # Handle MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip() if col[1] else col[0] for col in df.columns.values]
        
        # Process each player row
        for idx, row in df.iterrows():
            player_name = str(row.iloc[0]).strip()
            
            # Skip invalid rows
            if pd.isna(player_name) or player_name == '' or player_name.lower() in ['player', 'nan']:
                continue
            
            # Extract data based on category
            player_data = {
                'match_id': match_id,
                'player_name': player_name,
                'team_id': team_id,
            }
            
            # Add category-specific fields
            if category == 'summary':
                player_data.update(extract_summary_fields(row))
            elif category == 'passing':
                player_data.update(extract_passing_fields(row))
            elif category == 'passing_types':
                player_data.update(extract_pass_types_fields(row))
            elif category == 'defense':
                player_data.update(extract_defense_fields(row))
            elif category == 'possession': 
                player_data.update(extract_possession_fields(row))
            elif category == 'misc':
                player_data.update(extract_misc_fields(row))
            
            players_data[player_name] = player_data
            
    except Exception as e:
        print(f"    ❌ Error extracting {category}: {e}")
    
    return players_data

def extract_summary_fields(row):
    """Extract summary category fields"""
    return {
        # Basic info
        'shirt_number': safe_extract(row, ['#', 'Unnamed: 1_level_0']),
        'position': safe_extract(row, ['Pos', 'Unnamed: 3_level_0']),
        'minutes_played': safe_extract(row, ['Min', 'Unnamed: 5_level_0']),
        
        # Performance stats
        'summary_perf_gls': safe_extract(row, ['Performance_Gls', 'Gls']),
        'summary_perf_ast': safe_extract(row, ['Performance_Ast', 'Ast']),
        'summary_perf_pk': safe_extract(row, ['Performance_PK', 'PK']),
        'summary_perf_pkatt': safe_extract(row, ['Performance_PKatt', 'PKatt']),
        'summary_perf_sh': safe_extract(row, ['Performance_Sh', 'Sh']),
        'summary_perf_sot': safe_extract(row, ['Performance_SoT', 'SoT']),
        'summary_perf_crdy': safe_extract(row, ['Performance_CrdY', 'CrdY']),
        'summary_perf_crdr': safe_extract(row, ['Performance_CrdR', 'CrdR']),
        'summary_perf_touches': safe_extract(row, ['Performance_Touches', 'Touches']),
        'summary_perf_tkl': safe_extract(row, ['Performance_Tkl', 'Tkl']),
        'summary_perf_int': safe_extract(row, ['Performance_Int', 'Int']),
        'summary_perf_blocks': safe_extract(row, ['Performance_Blocks', 'Blocks']),
        
        # Expected stats
        'summary_exp_xg': safe_extract(row, ['Expected_xG', 'xG']),
        'summary_exp_npxg': safe_extract(row, ['Expected_npxG', 'npxG']),
        'summary_exp_xag': safe_extract(row, ['Expected_xAG', 'xAG']),
        
        # SCA/GCA
        'summary_sca_sca': safe_extract(row, ['SCA_SCA', 'SCA']),
        'summary_sca_gca': safe_extract(row, ['SCA_GCA', 'GCA']),
        
        # Passing snippet
        'summary_pass_cmp': safe_extract(row, ['Passes_Cmp', 'Cmp']),
        'summary_pass_att': safe_extract(row, ['Passes_Att', 'Att']),
        'summary_pass_cmp_pct': safe_extract(row, ['Passes_Cmp%', 'Cmp%']),
        'summary_pass_prgp': safe_extract(row, ['Passes_PrgP', 'PrgP']),
        
        # Carries & Take-ons
        'summary_carry_carries': safe_extract(row, ['Carries_Carries', 'Carries']),
        'summary_carry_prgc': safe_extract(row, ['Carries_PrgC', 'PrgC']),
        'summary_take_att': safe_extract(row, ['Take-Ons_Att', 'Take-Ons Att']),
        'summary_take_succ': safe_extract(row, ['Take-Ons_Succ', 'Take-Ons Succ']),
    }

def extract_passing_fields(row):
    """Extract detailed passing fields"""
    return {
        # Totals
        'passing_total_cmp': safe_extract(row, ['Total_Cmp', 'Cmp']),
        'passing_total_att': safe_extract(row, ['Total_Att', 'Att']),
        'passing_total_cmp_pct': safe_extract(row, ['Total_Cmp%', 'Cmp%']),
        'passing_total_totdist': safe_extract(row, ['Total_TotDist', 'TotDist']),
        'passing_total_prgdist': safe_extract(row, ['Total_PrgDist', 'PrgDist']),
        
        # Range splits
        'passing_short_cmp': safe_extract(row, ['Short_Cmp', 'Short Cmp']),
        'passing_short_att': safe_extract(row, ['Short_Att', 'Short Att']),
        'passing_short_cmp_pct': safe_extract(row, ['Short_Cmp%', 'Short Cmp%']),
        'passing_medium_cmp': safe_extract(row, ['Medium_Cmp', 'Medium Cmp']),
        'passing_medium_att': safe_extract(row, ['Medium_Att', 'Medium Att']),
        'passing_medium_cmp_pct': safe_extract(row, ['Medium_Cmp%', 'Medium Cmp%']),
        'passing_long_cmp': safe_extract(row, ['Long_Cmp', 'Long Cmp']),
        'passing_long_att': safe_extract(row, ['Long_Att', 'Long Att']),
        'passing_long_cmp_pct': safe_extract(row, ['Long_Cmp%', 'Long Cmp%']),
        
        # Value add
        'passing_ast': safe_extract(row, ['Expected_Ast', 'Ast']),
        'passing_xag': safe_extract(row, ['Expected_xAG', 'xAG']),
        'passing_xa': safe_extract(row, ['Expected_xA', 'xA']),
        'passing_kp': safe_extract(row, ['KP', 'Key Passes']),
        'passing_final_third': safe_extract(row, ['1/3', 'Final Third']),
        'passing_ppa': safe_extract(row, ['PPA', 'Penalty Area']),
        'passing_crspa': safe_extract(row, ['CrsPA', 'Cross Penalty Area']),
        'passing_prgp': safe_extract(row, ['PrgP', 'Progressive']),
    }

def extract_pass_types_fields(row):
    """Extract pass types fields"""
    return {
        'pass_types_att': safe_extract(row, ['Pass Types_Att', 'Att']),
        'pass_types_live': safe_extract(row, ['Pass Types_Live', 'Live']),
        'pass_types_dead': safe_extract(row, ['Pass Types_Dead', 'Dead']),
        'pass_types_fk': safe_extract(row, ['Pass Types_FK', 'FK']),
        'pass_types_tb': safe_extract(row, ['Pass Types_TB', 'TB']),
        'pass_types_sw': safe_extract(row, ['Pass Types_Sw', 'Sw']),
        'pass_types_crs': safe_extract(row, ['Pass Types_Crs', 'Crs']),
        'pass_types_ti': safe_extract(row, ['Pass Types_TI', 'TI']),
        'pass_types_ck': safe_extract(row, ['Pass Types_CK', 'CK']),
        
        # Corner kicks
        'corner_in': safe_extract(row, ['Corner Kicks_In', 'In']),
        'corner_out': safe_extract(row, ['Corner Kicks_Out', 'Out']),
        'corner_str': safe_extract(row, ['Corner Kicks_Str', 'Str']),
        
        # Outcomes
        'pass_outcome_cmp': safe_extract(row, ['Outcomes_Cmp', 'Cmp']),
        'pass_outcome_off': safe_extract(row, ['Outcomes_Off', 'Off']),
        'pass_outcome_blocks': safe_extract(row, ['Outcomes_Blocks', 'Blocks']),
    }

def extract_defense_fields(row):
    """Extract defensive actions fields"""
    return {
        # Tackles
        'def_tkl': safe_extract(row, ['Tackles_Tkl', 'Tkl']),
        'def_tklw': safe_extract(row, ['Tackles_TklW', 'TklW']),
        'def_tkl_def_3rd': safe_extract(row, ['Tackles_Def 3rd', 'Def 3rd']),
        'def_tkl_mid_3rd': safe_extract(row, ['Tackles_Mid 3rd', 'Mid 3rd']),
        'def_tkl_att_3rd': safe_extract(row, ['Tackles_Att 3rd', 'Att 3rd']),
        
        # Challenges
        'def_chal_tkl': safe_extract(row, ['Challenges_Tkl', 'Tkl']),
        'def_chal_att': safe_extract(row, ['Challenges_Att', 'Att']),
        'def_chal_tkl_pct': safe_extract(row, ['Challenges_Tkl%', 'Tkl%']),
        'def_chal_lost': safe_extract(row, ['Challenges_Lost', 'Lost']),
        
        # Blocks
        'def_blocks_total': safe_extract(row, ['Blocks_Blocks', 'Blocks']),
        'def_blocks_sh': safe_extract(row, ['Blocks_Sh', 'Sh']),
        'def_blocks_pass': safe_extract(row, ['Blocks_Pass', 'Pass']),
        
        # Other
        'def_int': safe_extract(row, ['Int', 'Interceptions']),
        'def_tkl_int': safe_extract(row, ['Tkl+Int', 'Tkl+Int']),
        'def_clr': safe_extract(row, ['Clr', 'Clearances']),
        'def_err': safe_extract(row, ['Err', 'Errors']),
    }

def extract_possession_fields(row):
    """Extract possession fields"""
    return {
        # Touches
        'poss_touches': safe_extract(row, ['Touches_Touches', 'Touches']),
        'poss_touches_def_pen': safe_extract(row, ['Touches_Def Pen', 'Def Pen']),
        'poss_touches_def_3rd': safe_extract(row, ['Touches_Def 3rd', 'Def 3rd']),
        'poss_touches_mid_3rd': safe_extract(row, ['Touches_Mid 3rd', 'Mid 3rd']),
        'poss_touches_att_3rd': safe_extract(row, ['Touches_Att 3rd', 'Att 3rd']),
        'poss_touches_att_pen': safe_extract(row, ['Touches_Att Pen', 'Att Pen']),
        'poss_touches_live': safe_extract(row, ['Touches_Live', 'Live']),
        
        # Take-ons
        'poss_take_att': safe_extract(row, ['Take-Ons_Att', 'Att']),
        'poss_take_succ': safe_extract(row, ['Take-Ons_Succ', 'Succ']),
        'poss_take_succ_pct': safe_extract(row, ['Take-Ons_Succ%', 'Succ%']),
        'poss_take_tkld': safe_extract(row, ['Take-Ons_Tkld', 'Tkld']),
        'poss_take_tkld_pct': safe_extract(row, ['Take-Ons_Tkld%', 'Tkld%']),
        
        # Carries
        'poss_carry_carries': safe_extract(row, ['Carries_Carries', 'Carries']),
        'poss_carry_totdist': safe_extract(row, ['Carries_TotDist', 'TotDist']),
        'poss_carry_prgdist': safe_extract(row, ['Carries_PrgDist', 'PrgDist']),
        'poss_carry_prgc': safe_extract(row, ['Carries_PrgC', 'PrgC']),
        'poss_carry_final_third': safe_extract(row, ['Carries_1/3', '1/3']),
        'poss_carry_cpa': safe_extract(row, ['Carries_CPA', 'CPA']),
        'poss_carry_mis': safe_extract(row, ['Carries_Mis', 'Mis']),
        'poss_carry_dis': safe_extract(row, ['Carries_Dis', 'Dis']),
        
        # Receiving
        'poss_rec_rec': safe_extract(row, ['Receiving_Rec', 'Rec']),
        'poss_rec_prgr': safe_extract(row, ['Receiving_PrgR', 'PrgR']),
    }

def extract_misc_fields(row):
    """Extract miscellaneous stats fields"""
    return {
        'misc_crdy': safe_extract(row, ['Performance_CrdY', 'CrdY']),
        'misc_crdr': safe_extract(row, ['Performance_CrdR', 'CrdR']),
        'misc_2crdy': safe_extract(row, ['Performance_2CrdY', '2CrdY']),
        'misc_fls': safe_extract(row, ['Performance_Fls', 'Fls']),
        'misc_fld': safe_extract(row, ['Performance_Fld', 'Fld']),
        'misc_off': safe_extract(row, ['Performance_Off', 'Off']),
        'misc_crs': safe_extract(row, ['Performance_Crs', 'Crs']),
        'misc_int': safe_extract(row, ['Performance_Int', 'Int']),
        'misc_tklw': safe_extract(row, ['Performance_TklW', 'TklW']),
        'misc_pkwon': safe_extract(row, ['Performance_PKwon', 'PKwon']),
        'misc_pkcon': safe_extract(row, ['Performance_PKcon', 'PKcon']),
        'misc_og': safe_extract(row, ['Performance_OG', 'OG']),
        'misc_recov': safe_extract(row, ['Performance_Recov', 'Recov']),
        
        # Aerial Duels
        'aerial_won': safe_extract(row, ['Aerial Duels_Won', 'Won']),
        'aerial_lost': safe_extract(row, ['Aerial Duels_Lost', 'Lost']),
        'aerial_won_pct': safe_extract(row, ['Aerial Duels_Won%', 'Won%']),
    }

def safe_extract(row, possible_columns):
    """Safely extract value from row using multiple possible column names"""
    for col in possible_columns:
        if col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip() != '':
                try:
                    # Try to convert to appropriate type
                    if '.' in str(value):
                        return float(value)
                    elif str(value).isdigit():
                        return int(value)
                    else:
                        return str(value).strip()
                except:
                    return str(value).strip()
    return None

def insert_comprehensive_data(players_data, db_path):
    """Insert comprehensive player data with all available fields"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Clear existing data
    match_id = players_data[0]['match_id']
    cursor.execute("DELETE FROM match_player WHERE match_id = ?", (match_id,))
    
    inserted_count = 0
    
    for player in players_data:
        try:
            # Generate unique ID
            match_player_id = str(uuid.uuid4())[:8]
            
            # Resolve season_id
            cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (player['match_id'],))
            result = cursor.fetchone()
            season_id = result[0] if result else None
            
            # Build comprehensive INSERT with all fields
            fields = [
                'match_player_id', 'match_id', 'player_id', 'player_name', 'team_id', 'team_name',
                'shirt_number', 'position', 'minutes_played', 'season_id'
            ]
            
            # Add all summary fields
            summary_fields = [
                'summary_perf_gls', 'summary_perf_ast', 'summary_perf_pk', 'summary_perf_pkatt',
                'summary_perf_sh', 'summary_perf_sot', 'summary_perf_crdy', 'summary_perf_crdr',
                'summary_perf_touches', 'summary_perf_tkl', 'summary_perf_int', 'summary_perf_blocks',
                'summary_exp_xg', 'summary_exp_npxg', 'summary_exp_xag', 'summary_sca_sca', 'summary_sca_gca',
                'summary_pass_cmp', 'summary_pass_att', 'summary_pass_cmp_pct', 'summary_pass_prgp',
                'summary_carry_carries', 'summary_carry_prgc', 'summary_take_att', 'summary_take_succ'
            ]
            fields.extend(summary_fields)
            
            # Add all passing fields
            passing_fields = [
                'passing_total_cmp', 'passing_total_att', 'passing_total_cmp_pct', 'passing_total_totdist', 'passing_total_prgdist',
                'passing_short_cmp', 'passing_short_att', 'passing_short_cmp_pct',
                'passing_medium_cmp', 'passing_medium_att', 'passing_medium_cmp_pct',
                'passing_long_cmp', 'passing_long_att', 'passing_long_cmp_pct',
                'passing_ast', 'passing_xag', 'passing_xa', 'passing_kp', 'passing_final_third', 'passing_ppa', 'passing_crspa', 'passing_prgp'
            ]
            fields.extend(passing_fields)
            
            # Add pass types fields
            pass_types_fields = [
                'pass_types_att', 'pass_types_live', 'pass_types_dead', 'pass_types_fk', 'pass_types_tb', 'pass_types_sw',
                'pass_types_crs', 'pass_types_ti', 'pass_types_ck', 'corner_in', 'corner_out', 'corner_str',
                'pass_outcome_cmp', 'pass_outcome_off', 'pass_outcome_blocks'
            ]
            fields.extend(pass_types_fields)
            
            # Add defensive fields
            def_fields = [
                'def_tkl', 'def_tklw', 'def_tkl_def_3rd', 'def_tkl_mid_3rd', 'def_tkl_att_3rd',
                'def_chal_tkl', 'def_chal_att', 'def_chal_tkl_pct', 'def_chal_lost',
                'def_blocks_total', 'def_blocks_sh', 'def_blocks_pass', 'def_int', 'def_tkl_int', 'def_clr', 'def_err'
            ]
            fields.extend(def_fields)
            
            # Add possession fields
            poss_fields = [
                'poss_touches', 'poss_touches_def_pen', 'poss_touches_def_3rd', 'poss_touches_mid_3rd',
                'poss_touches_att_3rd', 'poss_touches_att_pen', 'poss_touches_live',
                'poss_take_att', 'poss_take_succ', 'poss_take_succ_pct', 'poss_take_tkld', 'poss_take_tkld_pct',
                'poss_carry_carries', 'poss_carry_totdist', 'poss_carry_prgdist', 'poss_carry_prgc',
                'poss_carry_final_third', 'poss_carry_cpa', 'poss_carry_mis', 'poss_carry_dis',
                'poss_rec_rec', 'poss_rec_prgr'
            ]
            fields.extend(poss_fields)
            
            # Add misc fields
            misc_fields = [
                'misc_crdy', 'misc_crdr', 'misc_2crdy', 'misc_fls', 'misc_fld', 'misc_off',
                'misc_crs', 'misc_int', 'misc_tklw', 'misc_pkwon', 'misc_pkcon', 'misc_og', 'misc_recov',
                'aerial_won', 'aerial_lost', 'aerial_won_pct'
            ]
            fields.extend(misc_fields)
            
            # Build values tuple
            values = []
            values.extend([
                match_player_id, player['match_id'], None, player['player_name'], 
                player['team_id'], None, player.get('shirt_number'), player.get('position'), 
                player.get('minutes_played'), season_id
            ])
            
            # Add all field values
            for field in summary_fields + passing_fields + pass_types_fields + def_fields + poss_fields + misc_fields:
                values.append(player.get(field))
            
            # Execute insert
            placeholders = ','.join(['?' for _ in values])
            insert_sql = f"INSERT INTO match_player ({','.join(fields)}) VALUES ({placeholders})"
            
            cursor.execute(insert_sql, values)
            inserted_count += 1
            
        except Exception as e:
            print(f"❌ Error inserting {player['player_name']}: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Inserted {inserted_count} players with comprehensive data")

def verify_comprehensive_results(match_id, db_path):
    """Verify comprehensive extraction results"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Count populated fields
    cursor.execute("SELECT * FROM match_player WHERE match_id = ? LIMIT 1", (match_id,))
    sample_row = cursor.fetchone()
    
    if sample_row:
        non_null_count = sum(1 for value in sample_row if value is not None)
        print(f"\n🎉 COMPREHENSIVE RESULTS:")
        print(f"Fields populated per player: {non_null_count}/126")
        print(f"Coverage: {non_null_count/126*100:.1f}%")
    
    # Show sample stats
    cursor.execute("""
        SELECT player_name, summary_perf_gls, passing_total_cmp, def_tkl, poss_touches, misc_fls
        FROM match_player WHERE match_id = ? AND player_name NOT LIKE '%Players%' LIMIT 3
    """, (match_id,))
    
    print(f"\nSample comprehensive data:")
    for row in cursor.fetchall():
        name, goals, passes, tackles, touches, fouls = row
        print(f"  {name}: Goals:{goals}, Passes:{passes}, Tackles:{tackles}, Touches:{touches}, Fouls:{fouls}")
    
    conn.close()

if __name__ == "__main__":
    extract_comprehensive_008e301f()