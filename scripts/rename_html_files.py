#!/usr/bin/env python3
"""
Rename HTML files to [match_id].html format
Extract match_id from canonical URL - one method for ALL files
"""

import re
import sqlite3
from pathlib import Path

def extract_match_id_from_html(file_path):
    """Extract match_id from canonical URL in HTML file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Only read first 5000 characters (canonical URL is in head)
            content = f.read(5000)
        
        # Look for canonical URL pattern
        pattern = r'<link rel="canonical" href="https://fbref\.com/en/matches/([a-f0-9]{8})/'
        match = re.search(pattern, content)
        
        if match:
            return match.group(1)
        
        return None
        
    except Exception as e:
        print(f"   ❌ Error reading {file_path.name}: {e}")
        return None

def validate_match_id(match_id, db_path='data/processed/nwsldata.db'):
    """Check if match_id exists in database"""
    try:
        conn = sqlite3.connect(db_path)
        result = conn.execute('SELECT 1 FROM match WHERE match_id = ?', (match_id,)).fetchone()
        conn.close()
        return result is not None
    except:
        return False

def rename_html_files(html_dir='data/raw_match_pages', dry_run=True):
    """Rename all HTML files to [match_id].html format"""
    html_dir = Path(html_dir)
    
    print(f"🔄 {'DRY RUN: ' if dry_run else ''}Renaming HTML files in {html_dir}")
    
    files = list(html_dir.glob('*.html'))
    print(f"   Found {len(files)} HTML files")
    
    results = {
        'success': [],
        'already_correct': [],
        'no_match_id': [],
        'invalid_match_id': [],
        'errors': []
    }
    
    for i, file_path in enumerate(files, 1):
        print(f"\r   Processing {i}/{len(files)}: {file_path.name[:50]}", end="", flush=True)
        
        # Check if already correctly named
        if re.match(r'^[a-f0-9]{8}\.html$', file_path.name):
            results['already_correct'].append(file_path.name)
            continue
        
        # Extract match_id
        match_id = extract_match_id_from_html(file_path)
        
        if not match_id:
            results['no_match_id'].append(file_path.name)
            continue
        
        # Validate match_id
        if not validate_match_id(match_id):
            results['invalid_match_id'].append((file_path.name, match_id))
            continue
        
        # Rename file
        new_name = f"{match_id}.html"
        new_path = html_dir / new_name
        
        # Check for naming conflicts
        if new_path.exists() and new_path != file_path:
            results['errors'].append((file_path.name, f"Conflict: {new_name} already exists"))
            continue
        
        if not dry_run:
            try:
                file_path.rename(new_path)
                results['success'].append((file_path.name, new_name))
            except Exception as e:
                results['errors'].append((file_path.name, str(e)))
        else:
            results['success'].append((file_path.name, new_name))
    
    print()  # New line after progress
    
    # Print summary
    print(f"\n📊 Rename Results:")
    print(f"   ✅ Successfully renamed:  {len(results['success'])}")
    print(f"   ✅ Already correct:       {len(results['already_correct'])}")
    print(f"   ❌ No match_id found:     {len(results['no_match_id'])}")
    print(f"   ❌ Invalid match_id:      {len(results['invalid_match_id'])}")
    print(f"   ❌ Errors:                {len(results['errors'])}")
    
    if results['no_match_id']:
        print(f"\n🔍 Files without match_id (first 5):")
        for filename in results['no_match_id'][:5]:
            print(f"   {filename}")
    
    if results['invalid_match_id']:
        print(f"\n🔍 Invalid match_ids (first 5):")
        for filename, match_id in results['invalid_match_id'][:5]:
            print(f"   {filename} → {match_id} (not in database)")
    
    if results['errors']:
        print(f"\n🔍 Errors (first 5):")
        for filename, error in results['errors'][:5]:
            print(f"   {filename}: {error}")
    
    total_successful = len(results['success']) + len(results['already_correct'])
    total_files = len(files)
    
    print(f"\n🎯 Final Result:")
    print(f"   📁 Properly named files: {total_successful}/{total_files} ({total_successful/total_files*100:.1f}%)")
    
    return results

def main():
    print("🎯 HTML File Renaming Tool")
    print("   Method: Extract match_id from canonical URL")
    
    # First run dry run
    print("\n=== DRY RUN ===")
    results = rename_html_files(dry_run=True)
    
    total_renames = len(results['success'])
    
    if total_renames > 0:
        print(f"\n💡 This will rename {total_renames} files")
        response = input("Proceed with actual renaming? (y/N): ").strip().lower()
        
        if response == 'y':
            print("\n=== ACTUAL RENAME ===")
            rename_html_files(dry_run=False)
        else:
            print("❌ Renaming cancelled")
    else:
        print("\n✅ No files need renaming!")

if __name__ == "__main__":
    main()