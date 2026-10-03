"""Simulate the on-prem branch master extract (silver reference product).

    python generate_ref_branch.py --date 2026-10-03                 # 10 active branches
    python generate_ref_branch.py --date 2026-10-10 --close KC01    # KC01 marked inactive
    python generate_ref_branch.py --date 2026-10-11 --broken        # one bad region -> whole file rejected
"""
import argparse
import csv

BRANCHES = [
    ("KL01", "Jalan Tun Razak", "Central", "Kuala Lumpur"),
    ("KL02", "Bangsar", "Central", "Kuala Lumpur"),
    ("PJ01", "Petaling Jaya", "Central", "Selangor"),
    ("PG01", "George Town", "Northern", "Penang"),
    ("IP01", "Ipoh", "Northern", "Perak"),
    ("JB01", "Johor Bahru", "Southern", "Johor"),
    ("ML01", "Melaka", "Southern", "Melaka"),
    ("KT01", "Kuala Terengganu", "East Coast", "Terengganu"),
    ("KK01", "Kota Kinabalu", "East Malaysia", "Sabah"),
    ("KC01", "Kuching", "East Malaysia", "Sarawak"),
]
COLUMNS = ["branch_code", "branch_name", "region", "state", "active", "effective_from"]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--close", nargs="*", default=[], help="branch codes to mark inactive")
    ap.add_argument("--broken", action="store_true", help="give one branch an invalid region")
    a = ap.parse_args()

    out = f"ref_branch_{a.date.replace('-', '')}.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for i, (code, name, region, state) in enumerate(BRANCHES):
            if a.broken and i == 0:
                region = "Klang Valley"          # not an allowed value
            w.writerow([code, name, region, state, str(code not in a.close).lower(), a.date])
    print(f"wrote {out}: {len(BRANCHES)} branches, closed={a.close}, broken={a.broken}")
