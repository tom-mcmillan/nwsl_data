#!/usr/bin/env python3
"""
Check which 2013 match HTML files are available
"""

import os

# List of 2013 match_ids (first 20 for testing)
match_ids_2013_sample = [
    "6aee226c", "5c187984", "eb172ca3", "d0426a07", "83edc9ff", "d5615e5b", "064fab50", "7284c984",
    "81481f61", "6a57c82e", "8883ea79", "fb569f13", "f8177893", "7ac1c0c8", "0ca050d4", "1a01081c",
    "b9cf7980", "0e4932ff", "12c17fb7", "8640ac6f"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2013_sample:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2013 Match HTML Files Status (Sample of {len(match_ids_2013_sample)}):")
print(f"  Total match_ids: {len(match_ids_2013_sample)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2013_sample)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")

if found_files:
    print(f"\nFound HTML files ({len(found_files)}):")
    for i, match_id in enumerate(found_files[:10]):  # Show first 10
        print(f"  {i+1:2d}. {match_id}")
    if len(found_files) > 10:
        print(f"  ... and {len(found_files)-10} more")