#!/usr/bin/env python3
"""
Process all 2017 season matches to populate match_player_summary statistics
Uses 2018 script since 2017 has identical 24-field format
"""

import subprocess
import time

# List of all 2017 match_ids
match_ids_2017 = [
    "bb09ce9f",
    "cfce4a7e",
    "60c0fef2",
    "e0a6d860",
    "eb7f25b6",
    "64ae00a8",
    "bde14f5d",
    "ab51dc2a",
    "b14ef42f",
    "a0785409",
    "aae9acb9",
    "5ecc20d9",
    "657ab5b5",
    "2bbf11bc",
    "258598f2",
    "9f6d6df5",
    "0d86f760",
    "11e0e304",
    "9fa14fb5",
    "2d9e0ef0",
    "40d29f66",
    "2f8861a3",
    "beaa3276",
    "ba8ffe87",
    "72856ea5",
    "169816e4",
    "aae24672",
    "6ff06b8e",
    "e1e5105f",
    "625276de",
    "cb36f524",
    "b8918216",
    "554b360b",
    "6245629c",
    "18fdb801",
    "83e9f6cd",
    "391bfb7d",
    "49e5362d",
    "34a9f7eb",
    "e493bc1b",
    "78894a6f",
    "eaf0b116",
    "ba381725",
    "acf83533",
    "5ada410d",
    "21aac0ca",
    "02765e6d",
    "bc4d18e2",
    "14d766d0",
    "563b9572",
    "d1f5bba9",
    "c919bc72",
    "94916b48",
    "d6daa6a9",
    "8aa2a6b5",
    "20f76f42",
    "9f3f16bd",
    "440a6ec7",
    "66a2ae77",
    "127db480",
    "8b6d410c",
    "0e9d2a77",
    "0df8900f",
    "56cb2952",
    "1193750f",
    "08f400cf",
    "b583fc1c",
    "48a4cf5c",
    "55034c4e",
    "3166599e",
    "df22842d",
    "2682e8f7",
    "aacb507b",
    "a72ceab9",
    "3534d09f",
    "fb592d13",
    "13ecb249",
    "83cce59c",
    "b9ef73c0",
    "04a87c26",
    "edd08e31",
    "37d5d942",
    "4f0323c2",
    "2ac3cfd3",
    "c7123f09",
    "eb456c25",
    "e265026c",
    "ef2c6549",
    "f8de4250",
    "1f835c73",
    "6cf91083",
    "2818ce94",
    "5798634c",
    "5c6f66f8",
    "b1d44080",
    "8cd6d492",
    "2f3dc8f1",
    "e12b3240",
    "cb78b384",
    "f4ef0410",
    "65cf244a",
    "e12416e1",
    "678925e4",
    "f1a2a96d",
    "50ef46e1",
    "3dfd780a",
    "f4967720",
    "1fcb0c10",
    "666e71f1",
    "137224e9",
    "f4b7003b",
    "083cf4ad",
    "4b7edd1f",
    "0a035412",
    "fa841c55",
    "8dd983d8",
    "0c666b52",
    "f2348044",
    "7b27fb60",
    "277a7676",
    "6ff3d866",
    "c91958d4",
    "5206d9f2",
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
    print("🚀 Starting 2017 season processing...")
    print(f"   Total matches: {len(match_ids_2017)}")
    print("   Expected records: 3,394")
    print("   📊 Using 2018-compatible script (identical 24-field format)")

    success_count = 0
    failed_matches = []
    total_records_updated = 0

    start_time = time.time()

    for i, match_id in enumerate(match_ids_2017):
        print(f"   Processing {i+1:3d}/{len(match_ids_2017)}: {match_id}...", end=" ")

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

    print("\n📊 2017 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2017)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    print("   🔄 Format compatibility: 2017 = 2018 (24 fields)")

    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"      - {match_id}")
        if len(failed_matches) > 10:
            print(f"      ... and {len(failed_matches)-10} more")

    return success_count, failed_matches, total_records_updated


if __name__ == "__main__":
    success_count, failed_matches, total_records = main()

    if success_count == len(match_ids_2017):
        print("\n🎉 100% SUCCESS! All 2017 matches processed successfully!")
        print("🔧 Successfully leveraged 2018 script compatibility!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")
