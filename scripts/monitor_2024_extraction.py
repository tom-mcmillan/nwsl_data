#!/usr/bin/env python3
"""
Monitor 2024 player stats extraction progress
"""

import sqlite3
import time
from datetime import datetime

def get_extraction_progress(db_path):
    """Get current extraction progress."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Total 2024 matches
    cursor.execute("SELECT COUNT(*) FROM match WHERE season_id = 2024")
    total_matches = cursor.fetchone()[0]
    
    # Matches with player stats
    cursor.execute("""
        SELECT COUNT(DISTINCT match_id) 
        FROM match_player 
        WHERE match_id IN (SELECT match_id FROM match WHERE season_id = 2024)
    """)
    matches_with_stats = cursor.fetchone()[0]
    
    # Total player records
    cursor.execute("""
        SELECT COUNT(*) 
        FROM match_player 
        WHERE match_id IN (SELECT match_id FROM match WHERE season_id = 2024)
    """)
    total_player_records = cursor.fetchone()[0]
    
    conn.close()
    
    coverage_pct = (matches_with_stats / total_matches) * 100
    remaining = total_matches - matches_with_stats
    
    return {
        'total_matches': total_matches,
        'extracted_matches': matches_with_stats,
        'remaining_matches': remaining,
        'coverage_percent': coverage_pct,
        'total_player_records': total_player_records,
        'avg_players_per_match': total_player_records / matches_with_stats if matches_with_stats > 0 else 0
    }

def main():
    """Monitor extraction progress."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    print("🔍 2024 NWSL Player Stats Extraction Monitor")
    print("=" * 50)
    
    try:
        while True:
            progress = get_extraction_progress(db_path)
            
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            print(f"\n[{timestamp}] Progress Update:")
            print(f"  ✅ Extracted: {progress['extracted_matches']}/{progress['total_matches']} matches ({progress['coverage_percent']:.1f}%)")
            print(f"  📊 Player records: {progress['total_player_records']:,}")
            print(f"  ⏳ Remaining: {progress['remaining_matches']} matches")
            print(f"  📈 Avg players/match: {progress['avg_players_per_match']:.1f}")
            
            if progress['remaining_matches'] == 0:
                print("\n🎉 EXTRACTION COMPLETE! All 2024 matches processed.")
                break
            
            # Wait 30 seconds before next check
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n\n👋 Monitoring stopped by user")
    except Exception as e:
        print(f"\n❌ Error monitoring extraction: {e}")

if __name__ == "__main__":
    main()