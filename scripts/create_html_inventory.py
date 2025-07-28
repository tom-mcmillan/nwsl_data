#!/usr/bin/env python3
"""
Create an inventory table of all HTML match files for systematic re-scraping management.

This script:
1. Creates an 'html_inventory' table in the database
2. Scans all HTML files in organized year directories
3. Extracts metadata: filename, year, match_id, status
4. Populates the inventory table for programmatic analysis
"""

import os
import re
import sqlite3
from pathlib import Path
from bs4 import BeautifulSoup
import argparse
from datetime import datetime

def create_html_inventory_table(db_path):
    """Create the html_inventory table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Drop table if exists (for clean rebuild)
    cursor.execute("DROP TABLE IF EXISTS html_inventory")
    
    # Create table
    cursor.execute("""
        CREATE TABLE html_inventory (
            inventory_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            year INTEGER,
            match_id TEXT,
            status TEXT NOT NULL,
            file_size INTEGER,
            error_message TEXT,
            has_player_stats BOOLEAN DEFAULT 0,
            has_match_summary BOOLEAN DEFAULT 0,
            team_names TEXT,
            match_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(filepath)
        )
    """)
    
    conn.commit()
    conn.close()
    print("Created html_inventory table")

def extract_match_id_from_content(filepath):
    """Extract match ID from HTML content."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Method 1: Look for canonical URL
        canonical = soup.find('link', rel='canonical')
        if canonical:
            href = canonical.get('href', '')
            match = re.search(r'/matches/([a-f0-9]{8}|[A-F0-9]{8})/', href)
            if match:
                return match.group(1)
        
        # Method 2: Look for match URLs in the page
        match_links = soup.find_all('a', href=re.compile(r'/matches/[a-f0-9A-F]{8}/'))
        if match_links:
            href = match_links[0].get('href', '')
            match = re.search(r'/matches/([a-f0-9A-F]{8})/', href)
            if match:
                return match.group(1)
        
        return None
        
    except Exception as e:
        print(f"Error extracting match ID from {filepath}: {e}")
        return None

def extract_match_id_from_filename(filename):
    """Extract match ID from filename patterns."""
    # Pattern 1: hex ID only (e.g., "20423258.html")
    if re.match(r'^[a-f0-9]{8}\.html$', filename, re.IGNORECASE):
        return filename.replace('.html', '')
    
    # Pattern 2: match_report_YEAR_HEXID.html
    match = re.search(r'match_report_\d{4}_([a-f0-9]{8})\.html', filename, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Pattern 3: Look for hex patterns in descriptive names
    match = re.search(r'([a-f0-9]{8})', filename, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None

def analyze_html_content(filepath):
    """Analyze HTML content to determine status and extract metadata."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Check file size
        file_size = os.path.getsize(filepath)
        
        # Check for rate limiting
        if 'Rate Limited Request (429 error)' in content:
            return {
                'status': 'rate_limited',
                'error_message': '429 Rate Limited Error',
                'has_player_stats': False,
                'has_match_summary': False,
                'team_names': None,
                'match_date': None,
                'file_size': file_size
            }
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Extract title for team names and date
        title_tag = soup.find('title')
        title_text = title_tag.get_text() if title_tag else ''
        
        # Extract team names from title
        team_names = None
        match_date = None
        if 'Match Report' in title_text:
            # Pattern: "Team1 vs. Team2 Match Report – Day Month DD, YYYY"
            match = re.search(r'(.+?)\s+vs\.?\s+(.+?)\s+Match Report.*?(\w+ \w+ \d+, \d{4})', title_text)
            if match:
                team_names = f"{match.group(1)} vs {match.group(2)}"
                match_date = match.group(3)
        
        # Check for player statistics tables
        has_player_stats = bool(soup.find('table', {'class': 'stats_table'}) or 
                               soup.find('table', id=re.compile(r'stats_.*_summary')))
        
        # Check for match summary content
        has_match_summary = bool(soup.find('div', {'class': 'score'}) or
                                soup.find('div', {'id': 'team_stats'}) or
                                'Final Score' in content)
        
        # Determine overall status
        if has_player_stats and has_match_summary:
            status = 'good'
            error_message = None
        elif has_match_summary:
            status = 'partial'
            error_message = 'Missing player statistics'
        elif 'Match Report' in title_text:
            status = 'minimal'
            error_message = 'Limited content available'
        else:
            status = 'unknown'
            error_message = 'Could not determine content quality'
        
        return {
            'status': status,
            'error_message': error_message,
            'has_player_stats': has_player_stats,
            'has_match_summary': has_match_summary,
            'team_names': team_names,
            'match_date': match_date,
            'file_size': file_size
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error_message': str(e),
            'has_player_stats': False,
            'has_match_summary': False,
            'team_names': None,
            'match_date': None,
            'file_size': os.path.getsize(filepath) if os.path.exists(filepath) else 0
        }

def scan_html_files(base_dir):
    """Scan all HTML files in organized directories."""
    base_path = Path(base_dir)
    html_files = []
    
    # Scan year directories
    for year_dir in base_path.iterdir():
        if year_dir.is_dir() and year_dir.name.isdigit():
            year = int(year_dir.name)
            for html_file in year_dir.glob("*.html"):
                html_files.append((html_file, year))
    
    # Scan special directories
    for special_dir in ['rate_limited_errors', 'unknown_year']:
        special_path = base_path / special_dir
        if special_path.exists():
            for html_file in special_path.glob("*.html"):
                html_files.append((html_file, None))
    
    return html_files

def populate_html_inventory(db_path, base_dir):
    """Populate the html_inventory table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    html_files = scan_html_files(base_dir)
    print(f"Found {len(html_files)} HTML files to process")
    
    processed = 0
    for filepath, year in html_files:
        try:
            filename = filepath.name
            relative_path = str(filepath.relative_to(Path(base_dir).parent.parent))
            
            # Extract match ID
            match_id = extract_match_id_from_filename(filename)
            if not match_id:
                match_id = extract_match_id_from_content(filepath)
            
            # Analyze content
            analysis = analyze_html_content(filepath)
            
            # Insert record
            cursor.execute("""
                INSERT OR REPLACE INTO html_inventory 
                (filename, filepath, year, match_id, status, file_size, error_message,
                 has_player_stats, has_match_summary, team_names, match_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                filename,
                relative_path,
                year,
                match_id,
                analysis['status'],
                analysis['file_size'],
                analysis['error_message'],
                analysis['has_player_stats'],
                analysis['has_match_summary'],
                analysis['team_names'],
                analysis['match_date']
            ))
            
            processed += 1
            if processed % 100 == 0:
                print(f"Processed {processed}/{len(html_files)} files...")
                conn.commit()
        
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
    
    conn.commit()
    
    # Generate summary
    cursor.execute("""
        SELECT status, COUNT(*) as count 
        FROM html_inventory 
        GROUP BY status 
        ORDER BY count DESC
    """)
    status_counts = cursor.fetchall()
    
    cursor.execute("""
        SELECT year, COUNT(*) as count 
        FROM html_inventory 
        WHERE year IS NOT NULL
        GROUP BY year 
        ORDER BY year
    """)
    year_counts = cursor.fetchall()
    
    cursor.execute("""
        SELECT COUNT(*) as total_files,
               COUNT(CASE WHEN match_id IS NOT NULL THEN 1 END) as files_with_match_id,
               COUNT(CASE WHEN status = 'good' THEN 1 END) as good_files,
               COUNT(CASE WHEN has_player_stats = 1 THEN 1 END) as files_with_stats
        FROM html_inventory
    """)
    summary = cursor.fetchone()
    
    conn.close()
    
    print(f"\n{'='*60}")
    print("HTML INVENTORY SUMMARY")
    print('='*60)
    print(f"Total files processed: {processed}")
    print(f"Files with match IDs: {summary[1]}")
    print(f"Good quality files: {summary[2]}")
    print(f"Files with player stats: {summary[3]}")
    
    print(f"\nFiles by status:")
    for status, count in status_counts:
        print(f"  {status}: {count}")
    
    print(f"\nFiles by year:")
    for year, count in year_counts:
        print(f"  {year}: {count}")

def main():
    parser = argparse.ArgumentParser(description='Create HTML file inventory')
    parser.add_argument('--db-path', 
                       default='data/processed/nwsldata.db',
                       help='Path to SQLite database')
    parser.add_argument('--html-dir', 
                       default='data/raw_match_pages',
                       help='Directory containing organized HTML files')
    
    args = parser.parse_args()
    
    # Convert relative paths to absolute
    script_dir = Path(__file__).parent.parent
    db_path = script_dir / args.db_path if not os.path.isabs(args.db_path) else Path(args.db_path)
    html_dir = script_dir / args.html_dir if not os.path.isabs(args.html_dir) else Path(args.html_dir)
    
    print(f"Database: {db_path}")
    print(f"HTML directory: {html_dir}")
    print("-" * 60)
    
    # Create table
    create_html_inventory_table(db_path)
    
    # Populate inventory
    populate_html_inventory(db_path, html_dir)
    
    print(f"\nHTML inventory table created successfully!")
    print(f"Query with: SELECT * FROM html_inventory WHERE status = 'good';")

if __name__ == "__main__":
    main()