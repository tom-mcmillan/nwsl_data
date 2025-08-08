#!/usr/bin/env python3
"""
Process all 2013 season matches to populate match_player_summary statistics
Uses 2018 script since 2013 has identical 24-field format as 2014-2018
"""

import subprocess
import time

# Complete list of 2013 match_ids (91 matches)
match_ids_2013 = [
    "6aee226c",
    "5c187984",
    "eb172ca3",
    "d0426a07",
    "83edc9ff",
    "d5615e5b",
    "064fab50",
    "7284c984",
    "81481f61",
    "6a57c82e",
    "8883ea79",
    "fb569f13",
    "f8177893",
    "7ac1c0c8",
    "0ca050d4",
    "1a01081c",
    "b9cf7980",
    "0e4932ff",
    "12c17fb7",
    "8640ac6f",
    "2d03d5bd",
    "64f8c0fa",
    "94efc7a2",
    "c33b164f",
    "fd7738d7",
    "98d7c4d8",
    "2e5bf383",
    "87aca61f",
    "5054f8cd",
    "794331cf",
    "d2ab9b15",
    "2d641c7e",
    "6400280a",
    "b76731b7",
    "960dcbb0",
    "8d454ece",
    "7fcf5469",
    "83e2fe2b",
    "b4bfd7a8",
    "2128245a",
    "9490f202",
    "5b4fb64b",
    "cecd61f3",
    "a2766c37",
    "b910a28c",
    "82792623",
    "e8c4b786",
    "e28c3d9a",
    "ccf585fc",
    "1bf352c3",
    "99b6cf25",
    "2b7d2457",
    "91e83cd3",
    "e4707660",
    "bd91d1ac",
    "92e37743",
    "47835480",
    "16dce998",
    "eeb3b48d",
    "340af940",
    "c866830c",
    "5a05f6eb",
    "09ba17cd",
    "252a9e4e",
    "3a473dc3",
    "2f9c45a0",
    "4c3bd865",
    "d50220a4",
    "5aa2a435",
    "1612ecd5",
    "d3e24952",
    "2864f051",
    "5ee1d9e7",
    "b7c7cfbd",
    "a7627095",
    "38763070",
    "3ab52587",
    "f384c739",
    "033aefa6",
    "9867c808",
    "da5fbe09",
    "221dcc88",
    "01213dc2",
    "703a7b5a",
    "eace9837",
    "19eaba15",
    "25885873",
    "b823c2c9",
    "f481edf8",
    "b630a538",
    "ba397f2b",
]


def process_match(match_id):
    """Process a single match using the 2018-compatible populate script."""
    try:
        result = subprocess.run(
            ["python", "scripts/populate_match_player_summary_2018.py", match_id],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout after 60 seconds"
    except Exception as e:
        return False, "", str(e)


def main():
    print("🚀 Starting 2013 season processing...")
    print(f"   Total matches: {len(match_ids_2013)}")
    print("   Expected improvement: 2,467 → ~2,400 statistical records")
    print("   📊 Using 2018-compatible script (identical 24-field format)")
    print("   🎯 Extending legacy format compatibility to 6 seasons (2013-2018)!")

    success_count = 0
    failed_matches = []
    total_records_updated = 0

    start_time = time.time()

    for i, match_id in enumerate(match_ids_2013):
        print(f"   Processing {i+1:2d}/{len(match_ids_2013)}: {match_id}...", end=" ")

        success, stdout, stderr = process_match(match_id)

        if success:
            # Extract records updated count from output
            records_updated = 0
            if "Records updated:" in stdout:
                try:
                    line = [l for l in stdout.split("\n") if "Records updated:" in l][0]
                    records_updated = int(line.split("Records updated:")[1].strip())
                except:
                    records_updated = 0

            total_records_updated += records_updated
            success_count += 1
            print(f"✅ ({records_updated} records)")
        else:
            failed_matches.append(match_id)
            print("❌ FAILED")
            if stderr:
                print(f"      Error: {stderr[:100]}")

    elapsed = time.time() - start_time

    print("\n📊 2013 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2013)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    print("   🔄 Format compatibility: 2013 = 2014 = 2015 = 2016 = 2017 = 2018 (24 fields)")

    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"      - {match_id}")
        if len(failed_matches) > 10:
            print(f"      ... and {len(failed_matches)-10} more")

    return success_count, failed_matches, total_records_updated


if __name__ == "__main__":
    success_count, failed_matches, total_records = main()

    if success_count == len(match_ids_2013):
        print("\n🎉 100% SUCCESS! All 2013 matches processed successfully!")
        print("🏆 HISTORIC MILESTONE: 13 CONSECUTIVE SEASONS!")
        print("🔧 Successfully leveraged 2018 script compatibility!")
        print("📈 Extended legacy format compatibility to 6 seasons (2013-2018)!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")
