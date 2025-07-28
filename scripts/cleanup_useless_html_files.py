#!/usr/bin/env python3
"""
Clean up useless HTML files (rate-limited and error pages) from the project.

This script:
1. Identifies rate-limited files with high confidence (file size = 110,414 bytes)
2. Verifies file content contains "Rate Limited Request" 
3. Safely deletes confirmed useless files
4. Updates html_inventory table
5. Provides detailed reporting of cleanup actions
"""

import os
import sqlite3
from pathlib import Path
import argparse
from datetime import datetime

def identify_useless_files(db_path):
    """Identify files that are confirmed useless with high confidence."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all rate-limited files (they're all exactly 110,414 bytes)
    cursor.execute("""
        SELECT inventory_id, filename, filepath, file_size, status
        FROM html_inventory 
        WHERE status = 'rate_limited' 
        AND file_size = 110414
        ORDER BY filename
    """)
    
    rate_limited_files = cursor.fetchall()
    
    # Also check for any other files with suspicious patterns
    cursor.execute("""
        SELECT inventory_id, filename, filepath, file_size, status
        FROM html_inventory 
        WHERE (error_message LIKE '%429%' OR error_message LIKE '%Rate Limited%')
        AND status != 'good'
        ORDER BY filename
    """)
    
    error_files = cursor.fetchall()
    
    conn.close()
    
    # Combine and deduplicate
    all_useless = {}
    for file_info in rate_limited_files + error_files:
        all_useless[file_info[0]] = file_info  # Use inventory_id as key to dedupe
    
    return list(all_useless.values())

def verify_file_is_useless(filepath):
    """Double-check that a file is actually useless by examining content."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(1000)  # Read first 1000 chars
        
        # Check for rate limiting indicators
        useless_indicators = [
            'Rate Limited Request (429 error)',
            'Rate Limited Request',
            '429 error',
            'sports-reference.com/429.html'
        ]
        
        for indicator in useless_indicators:
            if indicator in content:
                return True, f"Contains: {indicator}"
        
        # If file is very small, it's probably not a real match report
        file_size = os.path.getsize(filepath)
        if file_size < 150000:  # Less than 150KB is suspicious
            return True, f"File too small: {file_size} bytes"
        
        return False, "File appears to have real content"
        
    except Exception as e:
        return True, f"Error reading file: {e}"

def delete_useless_files(useless_files, db_path, dry_run=True):
    """Delete confirmed useless files and update database."""
    
    print(f"🔍 Found {len(useless_files)} potentially useless files")
    print("=" * 60)
    
    # Verify each file
    confirmed_useless = []
    false_positives = []
    
    for inventory_id, filename, filepath, file_size, status in useless_files:
        full_path = Path(filepath) if Path(filepath).is_absolute() else Path(filepath)
        
        if not full_path.exists():
            print(f"⚠️  File not found: {filepath}")
            confirmed_useless.append((inventory_id, filename, filepath, "File not found"))
            continue
        
        is_useless, reason = verify_file_is_useless(full_path)
        
        if is_useless:
            confirmed_useless.append((inventory_id, filename, filepath, reason))
            print(f"🗑️  USELESS: {filename} ({reason})")
        else:
            false_positives.append((inventory_id, filename, filepath, reason))
            print(f"✅ KEEP: {filename} ({reason})")
    
    print(f"\n📊 VERIFICATION RESULTS:")
    print(f"Confirmed useless: {len(confirmed_useless)}")
    print(f"False positives: {len(false_positives)}")
    
    if not confirmed_useless:
        print("🎉 No useless files found to delete!")
        return 0
    
    if dry_run:
        print(f"\n🔒 DRY RUN - Would delete {len(confirmed_useless)} files")
        print("Use --execute to actually delete files")
        return 0
    
    # Actually delete files and update database
    print(f"\n🗑️  DELETING {len(confirmed_useless)} USELESS FILES...")
    
    deleted_count = 0
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for inventory_id, filename, filepath, reason in confirmed_useless:
        try:
            # Delete physical file
            full_path = Path(filepath) if Path(filepath).is_absolute() else Path(filepath)
            if full_path.exists():
                full_path.unlink()
                print(f"   ✅ Deleted: {filename}")
            
            # Remove from database
            cursor.execute("DELETE FROM html_inventory WHERE inventory_id = ?", (inventory_id,))
            deleted_count += 1
            
        except Exception as e:
            print(f"   ❌ Failed to delete {filename}: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"\n🎉 Successfully deleted {deleted_count} useless files!")
    return deleted_count

def cleanup_empty_directories(base_dir):
    """Remove empty directories after file deletion."""
    base_path = Path(base_dir)
    
    removed_dirs = []
    for dir_path in base_path.rglob("*"):
        if dir_path.is_dir():
            try:
                if not any(dir_path.iterdir()):  # Directory is empty
                    dir_path.rmdir()
                    removed_dirs.append(str(dir_path))
                    print(f"🗂️  Removed empty directory: {dir_path}")
            except OSError:
                pass  # Directory not empty or other error
    
    return removed_dirs

def generate_cleanup_report(db_path):
    """Generate a report of current file status after cleanup."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT status, COUNT(*) as count, 
               ROUND(AVG(file_size), 0) as avg_size,
               MIN(file_size) as min_size,
               MAX(file_size) as max_size
        FROM html_inventory 
        GROUP BY status 
        ORDER BY count DESC
    """)
    
    print(f"\n📊 POST-CLEANUP STATUS REPORT:")
    print("=" * 60)
    print("Status       | Count | Avg Size | Min Size | Max Size")
    print("-" * 60)
    
    total_files = 0
    for status, count, avg_size, min_size, max_size in cursor.fetchall():
        total_files += count
        print(f"{status:<12} | {count:>5} | {avg_size:>8.0f} | {min_size:>8} | {max_size:>8}")
    
    print("-" * 60)
    print(f"{'TOTAL':<12} | {total_files:>5}")
    
    # Get year breakdown for good files
    cursor.execute("""
        SELECT year, COUNT(*) as count
        FROM html_inventory 
        WHERE status = 'good' AND year IS NOT NULL
        GROUP BY year 
        ORDER BY year
    """)
    
    print(f"\n📅 GOOD FILES BY YEAR:")
    for year, count in cursor.fetchall():
        print(f"  {year}: {count} files")
    
    conn.close()

def main():
    parser = argparse.ArgumentParser(description='Clean up useless HTML files')
    parser.add_argument('--db-path', 
                       default='data/processed/nwsldata.db',
                       help='Path to SQLite database')
    parser.add_argument('--base-dir',
                       default='data/raw_match_pages',
                       help='Base directory containing HTML files')
    parser.add_argument('--execute', 
                       action='store_true',
                       help='Actually delete files (default is dry run)')
    parser.add_argument('--cleanup-dirs',
                       action='store_true',
                       help='Also remove empty directories after deletion')
    
    args = parser.parse_args()
    
    # Convert paths
    script_dir = Path(__file__).parent.parent
    db_path = script_dir / args.db_path if not Path(args.db_path).is_absolute() else Path(args.db_path)
    base_dir = script_dir / args.base_dir if not Path(args.base_dir).is_absolute() else Path(args.base_dir)
    
    print(f"🗄️  Database: {db_path}")
    print(f"📁 Base directory: {base_dir}")
    print(f"🎯 Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print()
    
    # Identify useless files
    useless_files = identify_useless_files(db_path)
    
    if not useless_files:
        print("🎉 No useless files found!")
        generate_cleanup_report(db_path)
        return 0
    
    # Delete files
    deleted_count = delete_useless_files(useless_files, db_path, dry_run=not args.execute)
    
    # Clean up empty directories if requested
    if args.execute and args.cleanup_dirs and deleted_count > 0:
        print(f"\n🗂️  Cleaning up empty directories...")
        removed_dirs = cleanup_empty_directories(base_dir)
        print(f"Removed {len(removed_dirs)} empty directories")
    
    # Generate final report
    generate_cleanup_report(db_path)
    
    print(f"\n🏁 Cleanup complete!")
    return 0

if __name__ == "__main__":
    exit(main())