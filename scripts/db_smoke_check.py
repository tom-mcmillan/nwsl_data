import os
import sqlite3
import sys

db_path = os.environ.get("NWSL_DB", "data/processed/nwsldata.db")
if not os.path.exists(db_path):
    print(f"DB missing at {db_path}")
    sys.exit(2)

con = sqlite3.connect(db_path)
cur = con.cursor()

tables = ["match_player_summary", "match", "player", "team"]
missing = []
for t in tables:
    row = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (t,),
    ).fetchone()
    if row is None:
        missing.append(t)

if missing:
    print("Missing tables:", ", ".join(missing))
    sys.exit(3)

top = cur.execute("""
SELECT player_name, SUM(goals) AS g
FROM match_player_summary
WHERE goals IS NOT NULL
GROUP BY player_name
ORDER BY g DESC
LIMIT 5
""").fetchall()

print("Top scorers (sample):", top)
print("DB smoke OK")
