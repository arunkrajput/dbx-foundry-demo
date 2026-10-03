"""Simulate the on-prem core banking extract.

    python generate_sample.py --date 2026-10-03 --rows 200
    python generate_sample.py --date 2026-10-04 --rows 200 --bad 30            # inject contract violations
    python generate_sample.py --date 2026-10-05 --rows 50 --unknown-branch 10  # valid in bronze, fails gold RI
"""
import argparse
import csv
import random
from datetime import date, timedelta

FIRST = ["Aisha", "Wei Ming", "Priya", "Ahmad", "Mei Ling", "Ravi", "Nurul", "Jason", "Siti", "Kumar"]
LAST = ["Rahman", "Tan", "Nair", "Abdullah", "Lim", "Krishnan", "Hassan", "Wong", "Ismail", "Raj"]
# (segment, weight, balance range)
SEGMENTS = [("mass", 0.75, (100, 20_000)), ("affluent", 0.20, (50_000, 500_000)), ("private", 0.05, (1e6, 8e6))]
# Must match the column order in contracts/customer_accounts.yml
# Must exist in ref_branch (see generate_ref_branch.py)
BRANCHES = ["KL01", "KL02", "PJ01", "PG01", "IP01", "JB01", "ML01", "KT01", "KK01", "KC01"]
COLUMNS = ["account_id", "customer_name", "ic_number", "segment", "branch_code", "account_open_date", "balance_myr"]


def fake_ic() -> str:
    d = date(1950, 1, 1) + timedelta(days=random.randint(0, 20000))
    return f"{d:%y%m%d}-{random.randint(1, 16):02d}-{random.randint(0, 9999):04d}"


def good_row(i: int) -> dict:
    seg, _, (lo, hi) = random.choices(SEGMENTS, weights=[s[1] for s in SEGMENTS])[0]
    return {
        "account_id": f"A{i:06d}",
        "customer_name": f"{random.choice(FIRST)} {random.choice(LAST)}",
        "ic_number": fake_ic(),
        "segment": seg,
        "branch_code": random.choice(BRANCHES),
        "account_open_date": str(date(2005, 1, 1) + timedelta(days=random.randint(0, 7500))),
        "balance_myr": f"{random.uniform(lo, hi):.2f}",
    }


def break_row(r: dict) -> dict:
    defect = random.choice(["no_id", "bad_ic", "bad_segment", "negative", "bad_date"])
    if defect == "no_id":
        r["account_id"] = ""
    elif defect == "bad_ic":
        r["ic_number"] = "12345"
    elif defect == "bad_segment":
        r["segment"] = "vip"
    elif defect == "negative":
        r["balance_myr"] = "-500.00"
    else:
        r["account_open_date"] = "31/02/2020"
    return r


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=str(date.today()), help="business date, YYYY-MM-DD")
    ap.add_argument("--rows", type=int, default=200)
    ap.add_argument("--bad", type=int, default=0, help="number of rows to corrupt")
    ap.add_argument("--start-id", type=int, default=1, help="first account number (overlap = updates)")
    ap.add_argument("--unknown-branch", type=int, default=0,
                    help="rows given a well-formed branch code that is not in ref_branch (ZZ99)")
    a = ap.parse_args()

    random.seed(a.date)
    rows = [good_row(a.start_id + i) for i in range(a.rows)]
    for r in random.sample(rows, k=min(a.bad, len(rows))):
        break_row(r)
    clean = [r for r in rows if r["account_id"] and r["ic_number"] != "12345"]
    for r in random.sample(clean, k=min(a.unknown_branch, len(clean))):
        r["branch_code"] = "ZZ99"

    out = f"customer_accounts_{a.date.replace('-', '')}.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {out}: {a.rows} rows, {a.bad} corrupted, {a.unknown_branch} unknown branch")
