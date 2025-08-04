#!/usr/bin/env python3
"""
Check which 2017 match HTML files are available
"""

import os

# List of 2017 match_ids
match_ids_2017 = [
    "bb09ce9f", "cfce4a7e", "60c0fef2", "e0a6d860", "eb7f25b6", "64ae00a8", "bde14f5d", "ab51dc2a",
    "b14ef42f", "a0785409", "aae9acb9", "5ecc20d9", "657ab5b5", "2bbf11bc", "258598f2", "9f6d6df5",
    "0d86f760", "11e0e304", "9fa14fb5", "2d9e0ef0", "40d29f66", "2f8861a3", "beaa3276", "ba8ffe87",
    "72856ea5", "169816e4", "aae24672", "6ff06b8e", "e1e5105f", "625276de", "cb36f524", "b8918216",
    "554b360b", "6245629c", "18fdb801", "83e9f6cd", "391bfb7d", "49e5362d", "34a9f7eb", "e493bc1b",
    "78894a6f", "eaf0b116", "ba381725", "acf83533", "5ada410d", "21aac0ca", "02765e6d", "bc4d18e2",
    "14d766d0", "563b9572", "d1f5bba9", "c919bc72", "94916b48", "d6daa6a9", "8aa2a6b5", "20f76f42",
    "9f3f16bd", "440a6ec7", "66a2ae77", "127db480", "8b6d410c", "0e9d2a77", "0df8900f", "56cb2952",
    "1193750f", "08f400cf", "b583fc1c", "48a4cf5c", "55034c4e", "3166599e", "df22842d", "2682e8f7",
    "aacb507b", "a72ceab9", "3534d09f", "fb592d13", "13ecb249", "83cce59c", "b9ef73c0", "04a87c26",
    "edd08e31", "37d5d942", "4f0323c2", "2ac3cfd3", "c7123f09", "eb456c25", "e265026c", "ef2c6549",
    "f8de4250", "1f835c73", "6cf91083", "2818ce94", "5798634c", "5c6f66f8", "b1d44080", "8cd6d492",
    "2f3dc8f1", "e12b3240", "cb78b384", "f4ef0410", "65cf244a", "e12416e1", "678925e4", "f1a2a96d",
    "50ef46e1", "3dfd780a", "f4967720", "1fcb0c10", "666e71f1", "137224e9", "f4b7003b", "083cf4ad",
    "4b7edd1f", "0a035412", "fa841c55", "8dd983d8", "0c666b52", "f2348044", "7b27fb60", "277a7676",
    "6ff3d866", "c91958d4", "5206d9f2"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2017:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2017 Match HTML Files Status:")
print(f"  Total match_ids: {len(match_ids_2017)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2017)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")