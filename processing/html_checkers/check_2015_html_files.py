#!/usr/bin/env python3
"""
Check which 2015 match HTML files are available
"""

import os

# List of 2015 match_ids
match_ids_2015 = [
    "c472965f", "5d762423", "478694f8", "c6e91034", "6d73cf64", "520134f4", "170d1dda", "c78c16a1",
    "9dcf496b", "3c2a99b1", "418e0e31", "e4c6e5a2", "e38fdb0f", "db1b8928", "f7ec5018", "e01f2ab2",
    "85ea774d", "611eb468", "1b276643", "3f955256", "4874cbfb", "f13fd918", "60713322", "29994120",
    "6c10cfdb", "3b6c58de", "0ab262a8", "d8f2bb6c", "4ccba30d", "b2f4ab2b", "53d69588", "541a43ed",
    "fb3fee9e", "d68ba0d9", "070a85c6", "0ac89923", "55840597", "0438ac6f", "5cbe3ea6", "2124d8de",
    "ad4385ca", "6c1960fe", "ca6b1e40", "9eae33e6", "0349e876", "f4e707e7", "bd617704", "b3536acf",
    "a8e5bb48", "1a84f24d", "bd7ec044", "7f3417a9", "caadbdc9", "7439eff5", "55934b05", "6c066abf",
    "0e7fddc2", "45ea9030", "61cc1e00", "565b1da5", "2d678cba", "889e323c", "f5582e60", "502ff272",
    "5c3b3b7a", "2bda25a1", "5ff5843f", "7aa0733c", "dc4da4e7", "88db5cc5", "decc5784", "1cf24c9d",
    "bc51c892", "50bd521d", "7239cc9a", "1bb660c2", "ade5efdc", "573e3558", "6c0afe0b", "fd03e664",
    "99b29ebc", "6fd000ec", "708f6bbe", "707d1ba5", "252a6a92", "266fc787", "08df8ebf", "5191e299",
    "be554b90", "5fe1d504", "093ed426", "9bd6f453", "c09cea95"
]

html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"

found_files = []
missing_files = []

for match_id in match_ids_2015:
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    if os.path.exists(html_file):
        found_files.append(match_id)
    else:
        missing_files.append(match_id)

print(f"2015 Match HTML Files Status:")
print(f"  Total match_ids: {len(match_ids_2015)}")
print(f"  Found HTML files: {len(found_files)}")
print(f"  Missing HTML files: {len(missing_files)}")
print(f"  Coverage: {len(found_files)/len(match_ids_2015)*100:.1f}%")

if missing_files:
    print(f"\nMissing HTML files ({len(missing_files)}):")
    for i, match_id in enumerate(missing_files):
        print(f"  {i+1:2d}. {match_id}")