#!/usr/bin/env python3
"""
Check which 2018 match HTML files are available
"""

import os

# List of 2018 match_ids
match_ids_2018 = [
    "aeb17e74", "3a815c4f", "73b7a256", "254105a8", "82737f56", "a17c0ef0", "9951420b", "9d713186",
    "66c8a43d", "b73fabbe", "0d694f3a", "e7c7c23c", "2906b503", "52649c9c", "387eff31", "2a696e31",
    "57fa7105", "98d7db91", "76114fb2", "8928d94b", "ea9f09e6", "3e51f799", "9d83af85", "6bc282b7",
    "fede5444", "ba422734", "a946c0e2", "36e97512", "79f8e9a0", "4d850dbb", "7b04d31a", "1f61e83b",
    "8a883753", "a1c035b7", "e6c9e5d4", "359bf5ad", "d231b19e", "002d9eb4", "f6ec8a8b", "8a947bd0",
    "6f73f9a8", "38851d0d", "c605cbfa", "57df675c", "ed0821bd", "9c06de8d", "bf237d24", "f1ce78a1",
    "8f38f319", "2d29c0fd", "2fd28c8a", "5360f412", "51a251e2", "4f767234", "1ff11e78", "9d12b00f",
    "10edb34e", "f27037ec", "0790a1bc", "3adf7469", "1e7cfb64", "bc4c3562", "94508a03", "31b27eae",
    "d2459fcd", "66ccdb0a", "cd4fe8d3", "e87befd2", "4aa78b99", "a61b70d1", "cd5433b9", "b5da8f35",
    "8fcf96e7", "74151c11", "874207c8", "53526198", "eddbe752", "84d6dca9", "f36366a9", "f25117a0",
    "f542d0eb", "21526888", "55471e47", "f7ee4334", "09657304", "5f19ba81", "465459ea", "e1e516b9",
    "6e0ca93c", "50f8bec2", "58d26f0b", "adcabd51", "30406f36", "c0b6a639", "ec411397", "d33ff0d8",
    "605fa6ac", "15f03025", "5238c761", "39dbe62f", "f484f6f6", "5a7b7125", "db9ea114", "092fe7ea",
    "e6091451", "e6d49a20", "783a65d6", "16744a84", "e17daaeb", "2c32f3d3", "22f04f44"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2018:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2018 Match HTML Files Status:")
print(f"  Total match_ids: {len(match_ids_2018)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2018)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")