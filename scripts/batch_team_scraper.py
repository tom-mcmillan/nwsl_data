#!/usr/bin/env python3
"""
Batch NWSL Match Team Statistics Scraper
Processes multiple match files from raw_match_pages directory
"""

import os
import sqlite3
from match_team_scraper import MatchTeamScraper
import glob

def get_match_id_from_filename(filename):
    """Extract match_id from filename"""
    basename = os.path.basename(filename)
    
    # Handle hex ID filenames like "0716dcc9.html" or "f09b4cef.html"
    if len(basename) == 13 and basename.endswith('.html') and len(basename[:-5]) == 8:
        return basename[:-5]
    
    # Handle match_report_YYYY_[hex].html format
    if basename.startswith('match_report_') and '_' in basename:
        parts = basename.replace('.html', '').split('_')
        if len(parts) >= 3:
            # Get the hex ID part (last part)
            hex_id = parts[-1]
            if len(hex_id) == 8:
                return hex_id
    
    # Handle descriptive filenames - generate ID from hash
    import hashlib
    return hashlib.md5(basename.encode()).hexdigest()[:8]

def check_existing_matches(db_path):
    """Get list of match_ids that already have team data"""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute('SELECT DISTINCT match_id FROM match_team')
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    return existing

def main():
    scraper = MatchTeamScraper()
    raw_pages_dir = 'data/raw_match_pages'
    
    # Get all HTML files
    html_files = glob.glob(os.path.join(raw_pages_dir, '*.html'))
    print(f"📊 Found {len(html_files)} HTML match files")
    
    # Check which matches already have data
    existing_matches = check_existing_matches(scraper.db_path)
    print(f"📋 Found {len(existing_matches)} matches with existing team data")
    
    # Process matches that don't have data yet
    processed = 0
    errors = 0
    
    for html_file in html_files[:100]:  # Process first 100 files
        match_id = get_match_id_from_filename(html_file)
        
        if match_id in existing_matches:
            print(f"⏭️  Skipping {match_id} - already processed")
            continue
        
        print(f"\n🔄 Processing {os.path.basename(html_file)}")
        print(f"   Match ID: {match_id}")
        
        try:
            success = scraper.scrape_match_team_stats(html_file, match_id)
            if success:
                processed += 1
                print(f"✅ Successfully processed match {match_id}")
            else:
                errors += 1
                print(f"❌ Failed to process match {match_id}")
        except Exception as e:
            errors += 1
            print(f"❌ Error processing {match_id}: {e}")
    
    print(f"\n📈 Batch processing complete!")
    print(f"   ✅ Successfully processed: {processed}")
    print(f"   ❌ Errors: {errors}")
    print(f"   📊 Total files processed: {processed + errors}")

if __name__ == "__main__":
    main()