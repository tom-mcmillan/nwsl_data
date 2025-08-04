#!/usr/bin/env python3
"""
Check which 2020 match HTML files are available
"""

import os

# List of 2020 match_ids
match_ids_2020 = [
    "760a26e0", "477d8522", "da5a0e99", "722a085f", "93804b58", "b4d1565f", "26684b63", "67a5da46",
    "610f69dd", "74ed987d", "bfbad8e1", "3266a287", "b0f251bd", "705aeb44", "6c66219b", "338f179a",
    "1ff4035b", "b4606770", "5edbe8f5", "8ecc4297", "2dc93287", "a172e221", "cdde1e7a", "0550dc14",
    "2330e071", "9ea41f98", "27bba0f5", "f7ab07b4", "81c16cad", "362bd167", "341cb0c8", "9a9a656b",
    "afbff619", "47752024", "640d698b", "eb4022c0", "e7d9f27c", "dc7cc573", "7f16970d", "794f4ccf",
    "bffd3e4c"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2020:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2020 Match HTML Files Status:")
print(f"  Total match_ids: {len(match_ids_2020)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2020)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")
        
print(f"\nFound files: {found_files}")