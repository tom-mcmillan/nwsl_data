#!/usr/bin/env python3
"""
Organize raw match page HTML files by year based on content parsing.

This script:
1. Reads HTML files from data/raw_match_pages/
2. Extracts year information from HTML content
3. Creates year-based subdirectories
4. Moves files to appropriate year folders
5. Handles failed downloads and edge cases
"""

import os
import re
import shutil
from pathlib import Path
from bs4 import BeautifulSoup
from collections import defaultdict
import argparse

def extract_year_from_html(filepath):
    """
    Extract year from HTML file content.
    
    Args:
        filepath (str): Path to HTML file
        
    Returns:
        tuple: (year, status, error_message)
            year: int or None
            status: 'success', 'error', 'rate_limited', 'no_year_found'
            error_message: str or None
    """
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Check for rate limited error first
        if 'Rate Limited Request (429 error)' in content:
            return None, 'rate_limited', '429 Rate Limited Error'
        
        # Parse HTML
        soup = BeautifulSoup(content, 'html.parser')
        
        # Method 1: Extract from title tag
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text()
            
            # Look for "Match Report – [Day] [Month] [Day], [YEAR]"
            match = re.search(r'Match Report.*?(\d{4})', title_text)
            if match:
                year = int(match.group(1))
                # Validate year range (NWSL started in 2013)
                if 2013 <= year <= 2030:
                    return year, 'success', None
        
        # Method 2: Look for other date patterns in the HTML
        # Search for meta tags or other date indicators
        meta_description = soup.find('meta', attrs={'name': 'Description'})
        if meta_description:
            content_attr = meta_description.get('content', '')
            match = re.search(r'(\d{4})', content_attr)
            if match:
                year = int(match.group(1))
                if 2013 <= year <= 2030:
                    return year, 'success', None
        
        # Method 3: Try filename extraction as fallback
        filename = os.path.basename(filepath)
        if 'match_report_' in filename:
            match = re.search(r'match_report_(\d{4})_', filename)
            if match:
                year = int(match.group(1))
                if 2013 <= year <= 2030:
                    return year, 'success', None
        
        # Method 4: Look for year in descriptive filenames
        if any(str(y) in filename for y in range(2013, 2031)):
            match = re.search(r'(201[3-9]|202[0-9])', filename)
            if match:
                year = int(match.group(1))
                return year, 'success', None
        
        return None, 'no_year_found', 'Could not extract year from content or filename'
        
    except Exception as e:
        return None, 'error', str(e)

def organize_files_by_year(source_dir, dry_run=True):
    """
    Organize HTML files by year.
    
    Args:
        source_dir (str): Path to directory containing HTML files
        dry_run (bool): If True, only show what would be done without moving files
    """
    source_path = Path(source_dir)
    if not source_path.exists():
        print(f"Error: Source directory {source_dir} does not exist")
        return
    
    # Get all HTML files
    html_files = list(source_path.glob("*.html"))
    print(f"Found {len(html_files)} HTML files to process\n")
    
    # Track results
    results = defaultdict(list)
    year_counts = defaultdict(int)
    
    # Process each file
    for i, filepath in enumerate(html_files, 1):
        if i % 100 == 0:
            print(f"Processed {i}/{len(html_files)} files...")
        
        year, status, error = extract_year_from_html(filepath)
        results[status].append((filepath.name, year, error))
        
        if year:
            year_counts[year] += 1
    
    # Print summary
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    
    print(f"Successfully processed: {len(results['success'])}")
    print(f"Rate limited errors: {len(results['rate_limited'])}")
    print(f"No year found: {len(results['no_year_found'])}")
    print(f"Other errors: {len(results['error'])}")
    
    print("\nFiles by year:")
    for year in sorted(year_counts.keys()):
        print(f"  {year}: {year_counts[year]} files")
    
    # Show problematic files
    if results['rate_limited']:
        print(f"\nRate limited files ({len(results['rate_limited'])}):")
        for filename, _, _ in results['rate_limited'][:10]:  # Show first 10
            print(f"  {filename}")
        if len(results['rate_limited']) > 10:
            print(f"  ... and {len(results['rate_limited']) - 10} more")
    
    if results['no_year_found']:
        print(f"\nFiles with no year found ({len(results['no_year_found'])}):")
        for filename, _, error in results['no_year_found'][:10]:  # Show first 10
            print(f"  {filename}")
        if len(results['no_year_found']) > 10:
            print(f"  ... and {len(results['no_year_found']) - 10} more")
    
    if results['error']:
        print(f"\nFiles with errors ({len(results['error'])}):")
        for filename, _, error in results['error'][:5]:  # Show first 5
            print(f"  {filename}: {error}")
        if len(results['error']) > 5:
            print(f"  ... and {len(results['error']) - 5} more")
    
    # Create directories and move files
    if not dry_run:
        print("\n" + "="*60)
        print("MOVING FILES")
        print("="*60)
        
        # Create year directories
        for year in year_counts.keys():
            year_dir = source_path / str(year)
            year_dir.mkdir(exist_ok=True)
            print(f"Created directory: {year_dir}")
        
        # Create special directories
        if results['rate_limited']:
            error_dir = source_path / "rate_limited_errors"
            error_dir.mkdir(exist_ok=True)
            print(f"Created directory: {error_dir}")
        
        if results['no_year_found'] or results['error']:
            unknown_dir = source_path / "unknown_year"
            unknown_dir.mkdir(exist_ok=True)
            print(f"Created directory: {unknown_dir}")
        
        # Move files
        moved_count = 0
        for status, file_list in results.items():
            for filename, year, error in file_list:
                source_file = source_path / filename
                
                if status == 'success' and year:
                    dest_dir = source_path / str(year)
                elif status == 'rate_limited':
                    dest_dir = source_path / "rate_limited_errors"
                else:
                    dest_dir = source_path / "unknown_year"
                
                dest_file = dest_dir / filename
                
                try:
                    shutil.move(str(source_file), str(dest_file))
                    moved_count += 1
                except Exception as e:
                    print(f"Error moving {filename}: {e}")
        
        print(f"\nSuccessfully moved {moved_count} files")
    
    else:
        print(f"\nDry run complete. Use --execute to actually move files.")

def main():
    parser = argparse.ArgumentParser(description='Organize match HTML files by year')
    parser.add_argument('--source-dir', 
                       default='data/raw_match_pages',
                       help='Source directory containing HTML files')
    parser.add_argument('--execute', 
                       action='store_true',
                       help='Actually move files (default is dry run)')
    
    args = parser.parse_args()
    
    # Convert relative path to absolute
    if not os.path.isabs(args.source_dir):
        script_dir = Path(__file__).parent.parent
        source_dir = script_dir / args.source_dir
    else:
        source_dir = Path(args.source_dir)
    
    print(f"Organizing files in: {source_dir}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print("-" * 60)
    
    organize_files_by_year(source_dir, dry_run=not args.execute)

if __name__ == "__main__":
    main()