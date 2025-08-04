#!/usr/bin/env python3
"""
Check which 2016 match HTML files are available
"""

import os

# List of 2016 match_ids
match_ids_2016 = [
    "0b641a7e", "2a7d2195", "ff1e7c30", "a0486da6", "75c25fca", "f0424777", "c5c5aa55", "ce38ef6c",
    "b514a2bf", "ca5bb662", "d85bd112", "9a7cca70", "7eba51ec", "a69cbaec", "62f30bfe", "9a5a8ae2",
    "e65c3820", "c6558d7d", "307958ca", "61cf8332", "b9586581", "157a3d0a", "639b391a", "c37fb478",
    "8bfa7c91", "6b1b6bac", "fd557bfc", "ed540940", "8c1b8361", "9624a028", "69a170d6", "8ea627dd",
    "28529962", "fff21e32", "f057de15", "70a27066", "2772caad", "73284fcd", "5ca528de", "c44e652b",
    "c517070e", "42046b2c", "7bd0ae51", "c1e29519", "0d797bd7", "47692ff2", "9e8bd26c", "137c4c4f",
    "b9d71721", "1f91c7ec", "b0ba7035", "9524abcc", "f193137b", "bfcbef4e", "13937440", "0c4b6be9",
    "e855070e", "ab55873f", "b87f47b5", "ee29032d", "08f3e4ef", "cf65e0d0", "d7011988", "e8ae45f5",
    "b9ba15e0", "009972a5", "e6f08fcf", "d600cea9", "e308d8d8", "4c7ee1cb", "bd79845f", "a45bfe72",
    "d6fc1bfa", "cd411b68", "aea0372f", "1e93d054", "ca38eecc", "e1c34c58", "af7f07e2", "eb621ced",
    "f142caa9", "4f202729", "804f2981", "f2614a92", "83cbdbe0", "85aa5c2b", "4bed2f1e", "2fbedf34",
    "f39f5cc1", "45b7e16a", "4d0c3cc1", "ec6febf9", "b4d08b96", "e60937a7", "2a58c0e7", "840295a3",
    "239f97b3", "6c9e55c3", "8e188aca", "d844b93b", "67767cee", "fbc0a335", "55312976"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2016:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2016 Match HTML Files Status:")
print(f"  Total match_ids: {len(match_ids_2016)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2016)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")