#!/usr/bin/env python3
"""
Batch NWSL Match Event Scraper
Processes match event data from raw_match_pages directory
"""

import os
import glob
from match_event_scraper import MatchEventScraper

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

def main():
    scraper = MatchEventScraper()
    raw_pages_dir = 'data/raw_match_pages'
    
    # Get all HTML files
    html_files = glob.glob(os.path.join(raw_pages_dir, '*.html'))
    print(f"📊 Found {len(html_files)} HTML match files")
    
    # Process files (start with recent ones, then do 2013)
    recent_files = [f for f in html_files if '2023' in f or '2021' in f]
    older_files = [f for f in html_files if '2013' in f]
    
    print(f"🔄 Processing {len(recent_files)} recent files and {len(older_files)} 2013 files")
    
    processed = 0
    errors = 0
    
    # Process recent files first (they have better data structure)
    for html_file in recent_files[:10]:  # Process first 10 recent files
        match_id = get_match_id_from_filename(html_file)
        
        print(f"\n🔄 Processing {os.path.basename(html_file)}")
        print(f"   Match ID: {match_id}")
        
        try:
            success = scraper.scrape_match_events(html_file, match_id)
            if success:
                processed += 1
                print(f"✅ Successfully processed match {match_id}")
            else:
                errors += 1
                print(f"❌ Failed to process match {match_id}")
        except Exception as e:
            errors += 1
            print(f"❌ Error processing {match_id}: {e}")
    
    # Now try some 2013 files
    print(f"\n🏆 Processing 2013 historical matches...")
    for html_file in older_files[:5]:  # Try first 5 older files
        match_id = get_match_id_from_filename(html_file)
        
        print(f"\n🔄 Processing {os.path.basename(html_file)}")
        print(f"   Match ID: {match_id}")
        
        try:
            success = scraper.scrape_match_events(html_file, match_id)
            if success:
                processed += 1
                print(f"✅ Successfully processed historical match {match_id}")
            else:
                errors += 1
                print(f"❌ Failed to process historical match {match_id}")
        except Exception as e:
            errors += 1
            print(f"❌ Error processing historical {match_id}: {e}")
    
    print(f"\n📈 Batch processing complete!")
    print(f"   ✅ Successfully processed: {processed}")
    print(f"   ❌ Errors: {errors}")
    print(f"   📊 Total files processed: {processed + errors}")

if __name__ == "__main__":
    main()