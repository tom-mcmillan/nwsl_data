#!/usr/bin/env python3
"""
Process all 2016 season matches to populate match_player_summary statistics
Uses 2018 script since 2016 has identical 24-field format as 2017-2018
"""

import subprocess
import sys
import time

# List of all 2016 match_ids
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
    print(f"🚀 Starting 2016 season processing...")
    print(f"   Total matches: {len(match_ids_2016)}")
    print(f"   Expected records: 2,830")
    print(f"   📊 Using 2018-compatible script (identical 24-field format)")
    print(f"   🎯 Aiming for 10th consecutive perfect season!")
    
    success_count = 0
    failed_matches = []
    total_records_updated = 0
    
    start_time = time.time()
    
    for i, match_id in enumerate(match_ids_2016):
        print(f"   Processing {i+1:3d}/{len(match_ids_2016)}: {match_id}...", end=" ")
        
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
    
    print(f"\n📊 2016 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2016)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    print(f"   🔄 Format compatibility: 2016 = 2017 = 2018 (24 fields)")
    
    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"      - {match_id}")
        if len(failed_matches) > 10:
            print(f"      ... and {len(failed_matches)-10} more")
    
    return success_count, failed_matches, total_records_updated

if __name__ == "__main__":
    success_count, failed_matches, total_records = main()
    
    if success_count == len(match_ids_2016):
        print(f"\n🎉 100% SUCCESS! All 2016 matches processed successfully!")
        print(f"🏆 HISTORIC MILESTONE: 10 CONSECUTIVE PERFECT SEASONS!")
        print(f"🔧 Successfully leveraged 2018 script compatibility!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")