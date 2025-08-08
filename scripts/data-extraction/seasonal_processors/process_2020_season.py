#!/usr/bin/env python3
"""
Process all 2020 season matches to populate match_player_summary statistics
"""

import subprocess
import time

# List of all 2020 match_ids
match_ids_2020 = [
    "760a26e0",
    "477d8522",
    "da5a0e99",
    "722a085f",
    "93804b58",
    "b4d1565f",
    "26684b63",
    "67a5da46",
    "610f69dd",
    "74ed987d",
    "bfbad8e1",
    "3266a287",
    "b0f251bd",
    "705aeb44",
    "6c66219b",
    "338f179a",
    "1ff4035b",
    "b4606770",
    "5edbe8f5",
    "8ecc4297",
    "2dc93287",
    "a172e221",
    "cdde1e7a",
    "0550dc14",
    "2330e071",
    "9ea41f98",
    "27bba0f5",
    "f7ab07b4",
    "81c16cad",
    "362bd167",
    "341cb0c8",
    "9a9a656b",
    "afbff619",
    "47752024",
    "640d698b",
    "eb4022c0",
    "e7d9f27c",
    "dc7cc573",
    "7f16970d",
    "794f4ccf",
    "bffd3e4c",
]


def process_match(match_id):
    """Process a single match using the populate script."""
    try:
        result = subprocess.run(
            ["python", "scripts/populate_match_player_summary.py", match_id],
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
    print("🚀 Starting 2020 season processing...")
    print(f"   Total matches: {len(match_ids_2020)}")

    success_count = 0
    failed_matches = []
    total_records_updated = 0

    start_time = time.time()

    for i, match_id in enumerate(match_ids_2020):
        print(f"   Processing {i+1:2d}/{len(match_ids_2020)}: {match_id}...", end=" ")

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

    print("\n📊 2020 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2020)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")

    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches:
            print(f"      - {match_id}")

    return success_count, failed_matches, total_records_updated


if __name__ == "__main__":
    success_count, failed_matches, total_records = main()

    if success_count == len(match_ids_2020):
        print("\n🎉 100% SUCCESS! All 2020 matches processed successfully!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")
