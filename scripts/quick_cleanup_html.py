#!/usr/bin/env python3
"""
Quick cleanup of obviously bad HTML files
Delete files that are clearly unusable based on simple checks
"""

import os
from pathlib import Path

def quick_cleanup_html_files(html_dir='data/raw_match_pages'):
    """Remove obviously bad HTML files"""
    html_dir = Path(html_dir)
    
    to_delete = []
    
    print(f"🔍 Quick scan of HTML files in {html_dir}")
    
    for file_path in html_dir.glob('*.html'):
        should_delete = False
        reason = ""
        
        # Check file size
        size = file_path.stat().st_size
        if size < 1000:  # Less than 1KB
            should_delete = True
            reason = f"Too small ({size} bytes)"
        
        # Check for error content in small files
        elif size < 10000:  # Less than 10KB, check content
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if any(error in content.lower() for error in [
                    '400 bad request', '404 not found', '403 forbidden',
                    '500 internal server error', 'cloudflare-nginx',
                    'request header or cookie too large'
                ]):
                    should_delete = True
                    reason = "Contains error message"
                
            except Exception as e:
                should_delete = True
                reason = f"Read error: {e}"
        
        if should_delete:
            to_delete.append((file_path, reason))
    
    print(f"📊 Found {len(to_delete)} files to delete")
    
    if to_delete:
        print("\nFiles to delete:")
        for file_path, reason in to_delete:
            print(f"   ❌ {file_path.name}: {reason}")
        
        response = input(f"\nDelete these {len(to_delete)} files? (y/N): ").strip().lower()
        
        if response == 'y':
            deleted = 0
            for file_path, reason in to_delete:
                try:
                    file_path.unlink()
                    deleted += 1
                    print(f"   🗑️ Deleted: {file_path.name}")
                except Exception as e:
                    print(f"   ❌ Failed to delete {file_path.name}: {e}")
            
            print(f"\n✅ Deleted {deleted} files")
            
            # Count remaining files
            remaining = len(list(html_dir.glob('*.html')))
            print(f"📁 Remaining HTML files: {remaining}")
            
        else:
            print("❌ Cleanup cancelled")
    else:
        total_files = len(list(html_dir.glob('*.html')))
        print(f"✅ No obvious problems found in {total_files} files")

if __name__ == "__main__":
    quick_cleanup_html_files()