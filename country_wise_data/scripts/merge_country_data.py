#!/usr/bin/env python3
"""
merge_country_data.py

Stage 3 of this pipeline. Combines the two independently-derived country
signals for each student into one file:

  - dial_country      -- guessed from the student's phone dial code
                          (dial_code_to_country.py, Stage 2 -- reads
                          ../input/Student-Export*.csv, writes
                          ../output/Student-Export_with_country.csv)
  - ip_driven_country  -- Edmingle's own geo-IP-based country for that
                          user (ip_driven_country_data.py, Stage 1 --
                          reads the live Edmingle API, writes
                          ../output/user_country_list.csv)

The two are joined on email (case-insensitive, whitespace-trimmed --
no other join key is used, since Contact Number is missing in ~29% of
Student-Export rows vs only ~0.02% missing Email). Every row from
Student-Export_with_country.csv is kept (a left join) -- ip_driven_country
is filled in wherever a matching email is found in user_country_list.csv,
left blank otherwise.

final_country: ip_driven_country WINS whenever it's present (it's a more
direct, current signal than a dial-code guess); dial_country is only used
as a fallback when there's no ip_driven match for that student at all.

Usage:
    python3 merge_country_data.py
    python3 merge_country_data.py --dial-input custom.csv --ip-input other.csv --output result.csv

Must be run after both Stage 1 (ip_driven_country_data.py) and Stage 2
(dial_code_to_country.py) have produced their output files -- this script
does not run either of them itself.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any local import below.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR.parent / "output"

DIAL_COUNTRY_SOURCE_COLUMN = "Derived Country (Dial Code)"
EMAIL_COLUMN = "Email"


def normalize_email(raw: str) -> str:
    return (raw or "").strip().lower()


def load_ip_driven_lookup(path: Path) -> dict[str, str]:
    """email (normalized) -> ip-driven country. Warns (does not fail) on
    duplicate emails in the ip-driven file -- keeps the first one seen."""
    lookup: dict[str, str] = {}
    duplicates = 0
    if not path.exists():
        print(f"WARNING: {path} does not exist -- treating as if Stage 1 produced zero rows "
              f"(every student's final_country will fall back to dial_country).")
        return lookup
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            print(f"WARNING: {path} has no header row -- treating as empty.")
            return lookup
        if "email" not in reader.fieldnames or "country" not in reader.fieldnames:
            sys.exit(
                f"{path} is missing an 'email' or 'country' column. "
                f"Found columns: {reader.fieldnames}"
            )
        for row in reader:
            email = normalize_email(row.get("email", ""))
            if not email:
                continue
            country = (row.get("country") or "").strip()
            if email in lookup:
                duplicates += 1
                continue
            lookup[email] = country
    if duplicates:
        print(f"WARNING: {duplicates} duplicate email(s) in {path} -- kept the first row seen for each.")
    return lookup


def read_dial_rows(path: Path, title_line_holder: list) -> tuple[list[str], list[dict]]:
    """Reads the Stage-2 output, tolerating the same optional leading junk
    title line dial_code_to_country.py itself tolerates on its own input."""
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        first_line = f.readline()
        looks_like_title_row = "," not in first_line.strip().strip('"')
        if looks_like_title_row:
            title_line_holder.append(first_line)
        else:
            f.seek(0)
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            sys.exit(f"Could not read a header row from {path}.")
        if DIAL_COUNTRY_SOURCE_COLUMN not in reader.fieldnames:
            sys.exit(
                f"{path} is missing the '{DIAL_COUNTRY_SOURCE_COLUMN}' column produced by "
                f"dial_code_to_country.py -- run Stage 2 first."
            )
        if EMAIL_COLUMN not in reader.fieldnames:
            sys.exit(f"{path} is missing an '{EMAIL_COLUMN}' column.")
        return list(reader.fieldnames), list(reader)


def merge(dial_path: Path, ip_path: Path, output_path: Path) -> None:
    ip_lookup = load_ip_driven_lookup(ip_path)

    title_line_holder: list = []
    fieldnames, rows = read_dial_rows(dial_path, title_line_holder)

    # Output column order: every original Student-Export column, with the
    # Stage-2 intermediate column renamed to dial_country (not duplicated),
    # then the two new columns appended.
    out_fieldnames = [
        "dial_country" if name == DIAL_COUNTRY_SOURCE_COLUMN else name
        for name in fieldnames
    ]
    out_fieldnames += ["ip_driven_country", "final_country"]

    total = 0
    matched_ip_driven = 0
    fell_back_to_dial = 0
    no_country_at_all = 0

    out_rows = []
    for row in rows:
        total += 1
        dial_country = (row.pop(DIAL_COUNTRY_SOURCE_COLUMN, "") or "").strip()
        row["dial_country"] = dial_country

        email = normalize_email(row.get(EMAIL_COLUMN, ""))
        ip_driven_country = ip_lookup.get(email, "") if email else ""
        row["ip_driven_country"] = ip_driven_country

        if ip_driven_country:
            final_country = ip_driven_country
            matched_ip_driven += 1
        elif dial_country:
            final_country = dial_country
            fell_back_to_dial += 1
        else:
            final_country = ""
            no_country_at_all += 1
        row["final_country"] = final_country

        out_rows.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        if title_line_holder:
            title = title_line_holder[0]
            f.write(title if title.endswith("\n") else title + "\n")
        writer = csv.DictWriter(f, fieldnames=out_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Read {total} rows from {dial_path}")
    print(f"Matched against {len(ip_lookup)} distinct emails in {ip_path}")
    print(f"  -> {matched_ip_driven} rows: final_country from ip_driven_country (Edmingle geo-IP)")
    print(f"  -> {fell_back_to_dial} rows: final_country fell back to dial_country (no ip-driven match)")
    print(f"  -> {no_country_at_all} rows: no country determined from either source")
    print(f"Wrote {output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dial-input", default=str(OUTPUT_DIR / "Student-Export_with_country.csv"),
                         help="Stage 2 output (default: ../output/Student-Export_with_country.csv)")
    parser.add_argument("--ip-input", default=str(OUTPUT_DIR / "user_country_list.csv"),
                         help="Stage 1 output (default: ../output/user_country_list.csv)")
    parser.add_argument("--output", default=str(OUTPUT_DIR / "merged_country_data.csv"),
                         help="Where to write the merged result (default: ../output/merged_country_data.csv)")
    args = parser.parse_args()

    dial_path = Path(args.dial_input)
    if not dial_path.exists():
        sys.exit(f"{dial_path} not found -- run dial_code_to_country.py (Stage 2) first.")

    merge(dial_path, Path(args.ip_input), Path(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
