#!/usr/bin/env python3
"""
merge_country_data.py

Stage 2 (after ip_driven_country_data.py). Reads the Edmingle student export and adds three columns to
every student, writing one file (output/merged_country_data.csv):

  dial_country       -- guessed from the "Contact Number Dial Code" column ("+91" -> "India") with
                        phonenumbers/pycountry. Where several countries share a code (+1, +44, +7) the
                        first/"main" region phonenumbers lists is used; "-", blank or unknown codes give "".
  ip_driven_country  -- Edmingle's own geo-IP country for that user, from Stage 1
                        (output/user_country_list.csv), joined on email (case-insensitive, trimmed --
                        Contact Number is missing in ~29% of rows, Email in ~0.02%). Every export row is
                        kept (a left join); blank where there is no match. The first row wins if Stage 1
                        has the same email twice.
  final_country      -- ip_driven_country when present (a direct, current signal), else dial_country.

The export has a junk title line ("Student's Export", possibly padded with commas) above the real header; it is
detected and written back unchanged. The export's own columns (including its "Country Name") are never modified.

Usage:
    python3 merge_country_data.py            # newest ../input/Student-Export*.csv + ../output/user_country_list.csv
    python3 merge_country_data.py --input export.csv --ip-input other.csv --output result.csv
"""

import argparse
import csv
import os
import re
import sys
from functools import lru_cache
from pathlib import Path

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set before any third-party import below.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import pycountry
from phonenumbers import COUNTRY_CODE_TO_REGION_CODE

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR.parent / "input"
OUTPUT_DIR = SCRIPT_DIR.parent / "output"
DEFAULT_INPUT_GLOB = "Student-Export*.csv"
EMAIL_COLUMN = "Email"
NEW_COLUMNS = ["dial_country", "ip_driven_country", "final_country"]


@lru_cache(maxsize=None)
def region_to_country_name(region_code: str) -> str:
    """ISO alpha-2 region code -> full country name via pycountry ('' for unknown; "001" is world/unknown)."""
    if not region_code or region_code == "001":
        return ""
    try:
        country = pycountry.countries.get(alpha_2=region_code)
    except LookupError:
        return ""
    return country.name if country else ""


@lru_cache(maxsize=None)
def dial_code_to_country(raw_dial_code: str) -> str:
    """'+91' -> 'India', '+1' -> 'United States', '-' / '' / None / unrecognized -> ''."""
    code = (raw_dial_code or "").strip()
    if code in ("", "-", "--", "N/A", "NA", "null", "None"):
        return ""
    code = code.lstrip("+").strip()
    if not code.isdigit():
        # Malformed rows (e.g. "+7 7"): fall back to the leading run of digits rather than dropping the row
        match = re.match(r"^\d+", code)
        if not match:
            return ""
        code = match.group(0)
    regions = COUNTRY_CODE_TO_REGION_CODE.get(int(code), [])
    return region_to_country_name(regions[0]) if regions else ""


def normalize_email(raw: str) -> str:
    return (raw or "").strip().lower()


def load_ip_driven_lookup(path: Path) -> dict[str, str]:
    """email (normalized) -> ip-driven country. A missing/empty file gives an empty lookup with a warning."""
    if not path.exists():
        print(f"WARNING: {path} does not exist -- treating as if Stage 1 produced zero rows "
              f"(every student's final_country will fall back to dial_country).")
        return {}
    lookup: dict[str, str] = {}
    duplicates = 0
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            print(f"WARNING: {path} has no header row -- treating as empty.")
            return {}
        if "email" not in reader.fieldnames or "country" not in reader.fieldnames:
            sys.exit(f"{path} is missing an 'email' or 'country' column. Found columns: {reader.fieldnames}")
        for row in reader:
            email = normalize_email(row.get("email", ""))
            if not email:
                continue
            if email in lookup:
                duplicates += 1
                continue
            lookup[email] = (row.get("country") or "").strip()
    if duplicates:
        print(f"WARNING: {duplicates} duplicate email(s) in {path} -- kept the first row seen for each.")
    return lookup


def find_default_input() -> Path:
    """The most recently modified Student-Export*.csv in ../input/ -- drop a fresh export there and re-run."""
    candidates = sorted(INPUT_DIR.glob(DEFAULT_INPUT_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        sys.exit(f"No --input given and no file matching '{DEFAULT_INPUT_GLOB}' found in {INPUT_DIR}.\n"
                 f"Either pass --input explicitly, or drop the export CSV into {INPUT_DIR}.")
    if len(candidates) > 1:
        print(f"Multiple exports found, using the most recent: {candidates[0].name}")
    return candidates[0]


def merge(input_path: Path, ip_path: Path, output_path: Path, dial_code_column: str, encoding: str) -> None:
    ip_lookup = load_ip_driven_lookup(ip_path)

    with input_path.open(newline="", encoding=encoding) as fh:
        first_line = fh.readline()
        # The junk title row has a single non-blank cell ("Student's Export", possibly padded with commas
        # when the file was re-saved from Excel); the real header row has many.
        is_title = sum(1 for cell in next(csv.reader([first_line]), []) if cell.strip()) == 1
        title_line = first_line if is_title else None
        if not is_title:
            fh.seek(0)
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            sys.exit(f"Could not read a header row from {input_path}.")
        fieldnames = list(reader.fieldnames)
        if dial_code_column not in fieldnames:
            sys.exit(f"Column '{dial_code_column}' not found in the CSV header.\nAvailable columns: {fieldnames}")
        if EMAIL_COLUMN not in fieldnames:
            sys.exit(f"{input_path} is missing an '{EMAIL_COLUMN}' column.")
        rows = list(reader)

    matched_ip = fell_back_to_dial = no_country = 0
    for row in rows:
        dial_country = dial_code_to_country(row.get(dial_code_column))
        email = normalize_email(row.get(EMAIL_COLUMN))
        ip_country = ip_lookup.get(email, "") if email else ""
        row.update(dial_country=dial_country, ip_driven_country=ip_country, final_country=ip_country or dial_country)
        if ip_country:
            matched_ip += 1
        elif dial_country:
            fell_back_to_dial += 1
        else:
            no_country += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    part = output_path.with_name(output_path.name + ".part")
    with part.open("w", newline="", encoding="utf-8") as fh:
        if title_line:
            fh.write(title_line if title_line.endswith("\n") else title_line + "\n")
        writer = csv.DictWriter(fh, fieldnames=fieldnames + NEW_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(part, output_path)

    print(f"Read {len(rows)} rows from {input_path}")
    print(f"Matched against {len(ip_lookup)} distinct emails in {ip_path}")
    print(f"  -> {matched_ip} rows: final_country from ip_driven_country (Edmingle geo-IP)")
    print(f"  -> {fell_back_to_dial} rows: final_country fell back to dial_country (no ip-driven match)")
    print(f"  -> {no_country} rows: no country determined from either source")
    print(f"Wrote {output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=None, help=f"The student export CSV (default: newest '{DEFAULT_INPUT_GLOB}' in ../input/)")
    parser.add_argument("--ip-input", default=str(OUTPUT_DIR / "user_country_list.csv"),
                        help="Stage 1 output (default: ../output/user_country_list.csv)")
    parser.add_argument("--output", default=str(OUTPUT_DIR / "merged_country_data.csv"),
                        help="Where to write the merged result (default: ../output/merged_country_data.csv)")
    parser.add_argument("--dial-code-column", default="Contact Number Dial Code",
                        help="Name of the dial-code column (default: 'Contact Number Dial Code')")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input file encoding (default: utf-8-sig, handles the Excel BOM)")
    args = parser.parse_args()

    input_path = Path(args.input) if args.input else find_default_input()
    if not input_path.exists():
        sys.exit(f"Input file not found: {input_path}")
    merge(input_path, Path(args.ip_input), Path(args.output), args.dial_code_column, args.encoding)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
