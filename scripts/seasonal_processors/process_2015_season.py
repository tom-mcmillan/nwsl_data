#!/usr/bin/env python3
"""
Process all 2015 season matches to populate match_player_summary statistics
Uses 2018 script since 2015 has identical 24-field format as 2016-2018
"""

import subprocess
import sys
import time

# List of all 2015 match_ids
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

def process_match(match_id):
    """Process a single match using the 2018-compatible populate script."""
    try:
        result = subprocess.run(
            ['python', 'scripts/populate_match_player_summary_2018.py', match_id],
            capture_output=True, text=True, timeout=60
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout after 60 seconds"
    except Exception as e:
        return False, "", str(e)

def main():
    print(f"🚀 Starting 2015 season processing...")
    print(f"   Total matches: {len(match_ids_2015)}")
    print(f"   Expected records: 2,530")
    print(f"   📊 Using 2018-compatible script (identical 24-field format)")
    print(f"   🎯 Aiming to extend perfect streak to 11 seasons!")
    
    success_count = 0
    failed_matches = []
    total_records_updated = 0
    
    start_time = time.time()
    
    for i, match_id in enumerate(match_ids_2015):
        print(f"   Processing {i+1:2d}/{len(match_ids_2015)}: {match_id}...", end=" ")
        
        success, stdout, stderr = process_match(match_id)
        
        if success:
            # Extract records updated count from output
            records_updated = 0
            if "Records updated:" in stdout:
                try:
                    line = [l for l in stdout.split('\n') if 'Records updated:' in l][0]
                    records_updated = int(line.split('Records updated:')[1].strip())
                except:
                    records_updated = 0
            
            total_records_updated += records_updated
            success_count += 1
            print(f"✅ ({records_updated} records)")
        else:
            failed_matches.append(match_id)
            print(f"❌ FAILED")
            if stderr:
                print(f"      Error: {stderr[:100]}")
    
    elapsed = time.time() - start_time
    
    print(f"\n📊 2015 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2015)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    print(f"   🔄 Format compatibility: 2015 = 2016 = 2017 = 2018 (24 fields)")
    
    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"      - {match_id}")
        if len(failed_matches) > 10:
            print(f"      ... and {len(failed_matches)-10} more")
    
    return success_count, failed_matches, total_records_updated

if __name__ == "__main__":
    success_count, failed_matches, total_records = main()
    
    if success_count == len(match_ids_2015):
        print(f"\n🎉 100% SUCCESS! All 2015 matches processed successfully!")
        print(f"🏆 EXTENDING THE STREAK: 11 CONSECUTIVE SEASONS!")
        print(f"🔧 Successfully leveraged 2018 script compatibility!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")