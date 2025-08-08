#!/usr/bin/env python3
"""
Analyze match data completeness in CSV files.
Identifies matches with complete team stats data (all 6 stat categories per team).
"""

from collections import defaultdict
from pathlib import Path

# Required stat categories for a complete match
REQUIRED_STATS = ["summary", "passing", "defense", "misc", "possession", "passing_types"]


def analyze_match_completeness(tables_dir):
    """
    Analyze which matches have complete team stats data.

    Returns:
    - complete_matches: list of match_ids with all required stats
    - incomplete_matches: dict of match_id -> missing stats
    - stats: overall statistics
    """

    tables_path = Path(tables_dir)
    complete_matches = []
    incomplete_matches = defaultdict(dict)

    # Track statistics
    total_matches = 0
    total_files = 0

    print("Scanning match folders for completeness...")
    print("Required stat types per team:", REQUIRED_STATS)
    print("-" * 60)

    # Process each match directory
    for match_dir in sorted(tables_path.iterdir()):
        if not match_dir.is_dir() or not match_dir.name.isalnum():
            continue

        match_id = match_dir.name
        total_matches += 1

        # Find all stat files for this match
        stat_files = list(match_dir.glob(f"{match_id}_stats_*_*.csv"))
        total_files += len(stat_files)

        # Group files by team_id and stat_type
        team_stats = defaultdict(set)

        for file_path in stat_files:
            # Parse filename: {match_id}_stats_{team_id}_{stat_type}.csv
            parts = file_path.stem.split("_")
            if len(parts) >= 4:
                team_id = parts[2]
                stat_type = "_".join(parts[3:])  # Handle compound names like passing_types
                team_stats[team_id].add(stat_type)

        # Check completeness for each team
        team_completeness = {}
        for team_id, stats_found in team_stats.items():
            missing_stats = set(REQUIRED_STATS) - stats_found
            team_completeness[team_id] = {
                "found": stats_found,
                "missing": missing_stats,
                "complete": len(missing_stats) == 0,
            }

        # Determine if match is complete (both teams have all required stats)
        if len(team_completeness) == 2:  # Should have exactly 2 teams
            all_teams_complete = all(team["complete"] for team in team_completeness.values())

            if all_teams_complete:
                complete_matches.append(match_id)
                print(f"✅ {match_id}: Complete ({len(team_stats)} teams)")
            else:
                incomplete_matches[match_id] = team_completeness
                missing_summary = []
                for team_id, info in team_completeness.items():
                    if info["missing"]:
                        missing_summary.append(f"{team_id}:{len(info['missing'])} missing")
                print(f"❌ {match_id}: Incomplete ({', '.join(missing_summary)})")
        else:
            # Wrong number of teams
            incomplete_matches[match_id] = team_completeness
            print(f"⚠️  {match_id}: Wrong team count ({len(team_completeness)} teams)")

    # Summary statistics
    complete_count = len(complete_matches)
    incomplete_count = len(incomplete_matches)
    completion_rate = (complete_count / total_matches * 100) if total_matches > 0 else 0

    print("\n" + "=" * 60)
    print("COMPLETENESS ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"Total matches analyzed: {total_matches}")
    print(f"Complete matches: {complete_count} ({completion_rate:.1f}%)")
    print(f"Incomplete matches: {incomplete_count}")
    print(f"Total stat files found: {total_files}")
    print(f"Average files per match: {total_files/total_matches:.1f}")

    return {
        "complete_matches": complete_matches,
        "incomplete_matches": dict(incomplete_matches),
        "stats": {
            "total_matches": total_matches,
            "complete_count": complete_count,
            "incomplete_count": incomplete_count,
            "completion_rate": completion_rate,
            "total_files": total_files,
        },
    }


def save_results(results, output_file="match_completeness_analysis.txt"):
    """Save analysis results to file."""

    with open(output_file, "w") as f:
        f.write("MATCH DATA COMPLETENESS ANALYSIS\n")
        f.write("=" * 50 + "\n\n")

        # Statistics
        stats = results["stats"]
        f.write(f"Total matches: {stats['total_matches']}\n")
        f.write(f"Complete matches: {stats['complete_count']} ({stats['completion_rate']:.1f}%)\n")
        f.write(f"Incomplete matches: {stats['incomplete_count']}\n\n")

        # Complete matches list
        f.write("COMPLETE MATCHES:\n")
        f.write("-" * 20 + "\n")
        for match_id in sorted(results["complete_matches"]):
            f.write(f"{match_id}\n")

        # Incomplete matches details
        f.write(f"\nINCOMPLETE MATCHES ({len(results['incomplete_matches'])}):\n")
        f.write("-" * 30 + "\n")
        for match_id, teams in sorted(results["incomplete_matches"].items()):
            f.write(f"\n{match_id}:\n")
            for team_id, info in teams.items():
                if info["missing"]:
                    f.write(f"  {team_id}: Missing {', '.join(sorted(info['missing']))}\n")
                else:
                    f.write(f"  {team_id}: Complete\n")

    print(f"Analysis saved to: {output_file}")


if __name__ == "__main__":
    # Path to tables directory
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"

    # Run analysis
    results = analyze_match_completeness(tables_dir)

    # Save results
    save_results(results)

    # Show some examples of complete matches
    complete_matches = results["complete_matches"]
    if complete_matches:
        print("\nFirst 10 complete matches:")
        for match_id in complete_matches[:10]:
            print(f"  {match_id}")

        if len(complete_matches) > 10:
            print(f"  ... and {len(complete_matches) - 10} more")
