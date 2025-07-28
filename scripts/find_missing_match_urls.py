#!/usr/bin/env python3
"""
Identify and generate URLs for missing NWSL match reports.

This script:
1. Compares database matches vs. HTML inventory
2. Extracts match IDs from rate-limited files
3. Generates FBref URLs for systematic re-scraping
4. Creates prioritized download lists
"""

import sqlite3
import re
from pathlib import Path
from collections import defaultdict
import argparse

def get_database_matches(db_path):
    """Get all matches from database with essential info."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            m.match_id,
            s.season_year,
            m.match_date,
            ht.team_name as home_team,
            at.team_name as away_team
        FROM match m
        JOIN season s ON m.season_id = s.season_id
        LEFT JOIN teams ht ON m.home_team_id = ht.team_id
        LEFT JOIN teams at ON m.away_team_id = at.team_id
        ORDER BY s.season_year, m.match_date
    """)
    
    matches = cursor.fetchall()
    conn.close()
    return matches

def get_html_inventory(db_path):
    """Get current HTML file inventory."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT match_id, year, status, filename
        FROM html_inventory
        WHERE match_id IS NOT NULL
    """)
    
    inventory = {}
    for match_id, year, status, filename in cursor.fetchall():
        inventory[match_id] = {
            'year': year,
            'status': status,
            'filename': filename
        }
    
    conn.close()
    return inventory

def extract_match_ids_from_rate_limited_files(db_path):
    """Extract match IDs from rate-limited filenames."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT filename, match_id
        FROM html_inventory 
        WHERE status = 'rate_limited'
        AND match_id IS NOT NULL
    """)
    
    rate_limited_ids = {}
    for filename, match_id in cursor.fetchall():
        # Extract year from filename pattern
        year_match = re.search(r'match_report_(\d{4})_', filename)
        if year_match:
            year = int(year_match.group(1))
            rate_limited_ids[match_id] = year
    
    conn.close()
    return rate_limited_ids

def generate_fbref_urls(match_ids_with_years):
    """Generate FBref URLs from match IDs."""
    urls = []
    
    for match_id, year in match_ids_with_years.items():
        # Standard FBref match URL pattern
        url = f"https://fbref.com/en/matches/{match_id}/"
        urls.append((match_id, year, url))
    
    return urls

def identify_missing_matches(db_path):
    """Identify matches that need HTML archival."""
    print("🔍 Analyzing missing match reports...")
    print("=" * 60)
    
    # Get data
    db_matches = get_database_matches(db_path)
    html_inventory = get_html_inventory(db_path)
    rate_limited_ids = extract_match_ids_from_rate_limited_files(db_path)
    
    print(f"Database matches: {len(db_matches)}")
    print(f"HTML files with match IDs: {len(html_inventory)}")
    print(f"Rate-limited files with IDs: {len(rate_limited_ids)}")
    
    # Analyze coverage
    missing_by_year = defaultdict(list)
    good_coverage = defaultdict(int)
    
    for match_id, season_year, match_date, home_team, away_team in db_matches:
        if match_id in html_inventory:
            status = html_inventory[match_id]['status']
            if status == 'good':
                good_coverage[season_year] += 1
            elif status == 'rate_limited':
                missing_by_year[season_year].append({
                    'match_id': match_id,
                    'date': match_date,
                    'matchup': f"{home_team} vs {away_team}",
                    'source': 'rate_limited_file'
                })
        else:
            # Match in database but no HTML file at all
            missing_by_year[season_year].append({
                'match_id': match_id,
                'date': match_date,
                'matchup': f"{home_team} vs {away_team}",
                'source': 'no_html_file'
            })
    
    # Summary by year
    print(f"\n📊 Coverage Summary:")
    print("Year | DB Matches | Good HTML | Missing | Coverage %")
    print("-" * 55)
    
    total_missing = 0
    for year in sorted(set(dict(missing_by_year).keys()) | set(good_coverage.keys())):
        db_count = len([m for m in db_matches if m[1] == year])
        good_count = good_coverage[year]
        missing_count = len(missing_by_year[year])
        coverage_pct = (good_count / db_count * 100) if db_count > 0 else 0
        
        print(f"{year} |     {db_count:3d}     |    {good_count:3d}    |   {missing_count:3d}   |   {coverage_pct:5.1f}%")
        total_missing += missing_count
    
    print("-" * 55)
    print(f"TOTAL MISSING: {total_missing} match reports")
    
    return missing_by_year, rate_limited_ids

def create_download_lists(missing_by_year, rate_limited_ids, output_dir):
    """Create prioritized download lists."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Priority 1: Recent years (2019-2025) with known match IDs
    priority_1 = []
    priority_2 = []  # Older years (2013-2018)
    priority_3 = []  # Matches without confirmed match IDs
    
    for year, matches in missing_by_year.items():
        for match in matches:
            match_id = match['match_id']
            url = f"https://fbref.com/en/matches/{match_id}/"
            
            entry = {
                'match_id': match_id,
                'year': year,
                'date': match['date'],
                'matchup': match['matchup'],
                'url': url,
                'filename': f"{match_id}.html"
            }
            
            if year >= 2019:
                priority_1.append(entry)
            elif year >= 2013:
                priority_2.append(entry)
            else:
                priority_3.append(entry)
    
    # Write download lists
    lists = [
        ('priority_1_recent_years.json', priority_1, "Recent years (2019+) - Highest success probability"),
        ('priority_2_older_years.json', priority_2, "Older years (2013-2018) - May need different approach"),
        ('priority_3_unknown_ids.json', priority_3, "Matches without confirmed IDs - Research needed")
    ]
    
    import json
    
    for filename, data, description in lists:
        if data:  # Only create file if there's data
            filepath = output_path / filename
            with open(filepath, 'w') as f:
                json.dump({
                    'description': description,
                    'count': len(data),
                    'matches': data
                }, f, indent=2)
            
            print(f"📝 Created {filepath} ({len(data)} matches)")
    
    # Create simple URL list for immediate scraping
    all_urls = [entry['url'] for entry in priority_1 + priority_2]
    url_file = output_path / 'missing_match_urls.txt'
    with open(url_file, 'w') as f:
        for url in all_urls:
            f.write(url + '\n')
    
    print(f"📝 Created {url_file} ({len(all_urls)} URLs)")
    
    return len(priority_1), len(priority_2), len(priority_3)

def main():
    parser = argparse.ArgumentParser(description='Find missing NWSL match report URLs')
    parser.add_argument('--db-path', 
                       default='data/processed/nwsldata.db',
                       help='Path to SQLite database')
    parser.add_argument('--output-dir',
                       default='data/missing_matches',
                       help='Directory to save download lists')
    
    args = parser.parse_args()
    
    # Convert relative paths
    script_dir = Path(__file__).parent.parent
    db_path = script_dir / args.db_path if not Path(args.db_path).is_absolute() else Path(args.db_path)
    output_dir = script_dir / args.output_dir if not Path(args.output_dir).is_absolute() else Path(args.output_dir)
    
    print(f"Database: {db_path}")
    print(f"Output directory: {output_dir}")
    print()
    
    # Analyze missing matches
    missing_by_year, rate_limited_ids = identify_missing_matches(db_path)
    
    # Create download lists
    print(f"\n📋 Creating Download Lists...")
    p1_count, p2_count, p3_count = create_download_lists(missing_by_year, rate_limited_ids, output_dir)
    
    print(f"\n🎯 Next Steps:")
    print(f"1. Start with Priority 1: {p1_count} recent matches (2019+)")
    print(f"2. Then Priority 2: {p2_count} older matches (2013-2018)")
    if p3_count > 0:
        print(f"3. Research Priority 3: {p3_count} matches with unclear IDs")
    
    print(f"\n🚀 Ready to begin systematic archival!")

if __name__ == "__main__":
    main()