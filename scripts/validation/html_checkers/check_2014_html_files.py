#!/usr/bin/env python3
"""
Check which 2014 match HTML files are available
"""

import os

# List of 2014 match_ids
match_ids_2014 = [
    "7239a666", "7ba0c3c1", "008e301f", "81b4c9d5", "d8b5b0d1", "9e8a54ad", "b5b00e0d", "95e1c7f8",
    "4b3ba6eb", "f926b2f9", "b9c7a9ae", "9fdfbc92", "7e1b42e4", "f1f8eb96", "ac4f0879", "c58e1bb2",
    "d93b2f45", "bd4dc2e9", "84e0b5c6", "d4e99bb3", "7e6de6ae", "7ab7b9be", "ce3b4b4f", "4ff45b34",
    "9823e37a", "a5b2fdaa", "87ab7a76", "1bd4c4d7", "e0327f5c", "67bb9c30", "a2b9d9b2", "e73cbe1b",
    "c7ad84b1", "c1d00c4f", "16ad16d3", "e5593c9b", "50ea6f32", "c2c37f2b", "0b4c0c04", "48b8a7f8",
    "9e9e47ba", "3d0b6cd7", "e6b6ab7c", "e8b0c877", "e2f5ec2e", "de7b8cf6", "4cbf5bf7", "e7d1a8b9",
    "23f26c6e", "9a27b2b4", "3b7cb6eb", "e9a73f4b", "e4e1b4c6", "5bd19c1f", "a3d9c08c", "cfc50daa",
    "7cd4eaec", "7e48d9ab", "bf98b6c6", "a8b91ae2", "4bce7334", "89f55cf1", "b6ed7e71", "e1f57c7b",
    "b1b6d967", "50c8a5f5", "5b4b7fa1", "e8e07b37", "e3f02ca4", "e3aa9b85", "3f8d3df5", "bb7b2975",
    "7ad3c0e9", "abe07b41", "40b8bd1f", "a7b7a1c3", "8f1d6eb0", "f037cc13", "07dedb30", "8e3b5a30",
    "b3de4b2e", "8b8b4a78", "43b9a33e", "7e1bc7a7", "36b5b6d5", "9c0b3c8f", "bf7cdc2c", "d88b2dec",
    "26ade36d", "8e7bb7c3", "c1c7a2ea", "4b7b97ce", "1e2dd36a", "a3c0c5a0", "4eb59bd1", "83c7dac3",
    "d5b2ab6b", "e07d6d5f", "bd9d8ba2", "3b7bb6be", "8b0aa9d3", "6bd7a4da", "03f1b6b1", "d1abb6f4",
    "ddc14bf3", "7ad7b2b7", "b3aa4b9e", "0ab6d9b7"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2014:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2014 Match HTML Files Status:")
print(f"  Total match_ids: {len(match_ids_2014)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2014)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")