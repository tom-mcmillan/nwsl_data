#!/usr/bin/env python3
"""
Check which 2019 match HTML files are available
"""

import os

# List of 2019 match_ids
match_ids_2019 = [
    "afcd583a", "67a57f59", "c9ec6863", "25004f7e", "9bf95ec5", "70b9c1b6", "87083ab3", "b66598e4",
    "ff7f188e", "8780d6a5", "b87d86b7", "ae49ad75", "85ba8579", "af3f157d", "5394bc1b", "efddeda8",
    "36c46e0d", "d53451cf", "86b6cc0a", "9a2b26a1", "23114faa", "37be7787", "d26ed7f0", "7f398f88",
    "cc8ebaaa", "0f1cb3d1", "4120af97", "c79ee9c4", "622f898e", "f8c8aea4", "6420bec8", "610a4c17",
    "1cef5979", "888d23a0", "bde3da3d", "f7b69a29", "6f3dc675", "1e61252e", "eb81709b", "ec9ceb9f",
    "7ee309e3", "05482155", "b30b11e9", "745bedf3", "49094e3a", "1520b6f5", "0262bb35", "b2b9405a",
    "51020dea", "8f3fbf96", "78fac894", "efcbf7b7", "2b75137d", "dbbdb47c", "4f5b874c", "e133d584",
    "6c56c1c8", "38048580", "125df7bb", "01cdf2c9", "f7ea6cf4", "3e2273da", "a0c570ff", "3903be81",
    "453b20ed", "3690a734", "073975b1", "6d4a68e6", "e4dad184", "202faad2", "67076783", "4aa7a9c5",
    "6536d5aa", "a6063bfa", "976b8d77", "e554a812", "a0d14941", "ef3f22f7", "3b62060e", "6f44cb0a",
    "2f8d4701", "3e58ee5e", "dd37453e", "6e16e67b", "b31185f6", "aa5085c0", "4d435ae3", "fdd56674",
    "8b7900bd", "508e1cf0", "1173feeb", "d6124086", "03f02a2d", "fba4e358", "1b8fd283", "2c25dcc1",
    "96746e28", "3653dfaf", "f33364ee", "4a9bc623", "d7245076", "731bfd8e", "f2a8492d", "a6299d40",
    "b3dca21a", "840ceaac", "3ccbf5a1", "9e58d38e", "6b7e06cd", "51f18293", "9f7344bb"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2019:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2019 Match HTML Files Status:")
print(f"  Total match_ids: {len(match_ids_2019)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2019)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")