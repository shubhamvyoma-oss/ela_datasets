#!/usr/bin/env python3
"""
dial_code_to_country.py

Reads an Edmingle student export CSV and adds a new column deriving the
country from the "Contact Number Dial Code" column (e.g. "+91" -> "India"),
using the same phonenumbers/pycountry-based logic used earlier in this
project. Does NOT touch or overwrite any existing "Country Name" column --
it only adds a new, clearly-named column alongside it.

Handles:
  - The Edmingle export's leading junk title line ("Student's Export")
    that sits above the real header row.
  - Dial codes stored as "-" or blank (no phone/dial code on file).
  - Multiple countries sharing one dial code (e.g. +1 -> US/Canada/etc.,
    +44 -> UK, +7 -> Russia/Kazakhstan) -- picks the primary/most common
    country for that code.

Usage:
    # Auto-picks the most recently modified "Student-Export*.csv" in this
    # pipeline's ../input/ folder -- drop a fresh export there and run:
    python3 dial_code_to_country.py

    # Or point it at a specific file / name the output / use a different
    # dial-code column name if a future export renames it:
    python3 dial_code_to_country.py --input "../input/Student-Export-18-09-2026_15_50_13.csv"
    python3 dial_code_to_country.py --input export.csv --output export_with_country.csv
    python3 dial_code_to_country.py --input export.csv --dial-code-column "Contact Number Dial Code"

Default input/output folders are resolved relative to this script's own
location (../input/ and ../output/), not the caller's current working
directory, matching the rest of ela_datasets/.
"""

import os
import sys

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any third-party import below.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import argparse
import csv
import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR.parent / "input"
OUTPUT_DIR = SCRIPT_DIR.parent / "output"

try:
    import phonenumbers
    from phonenumbers import COUNTRY_CODE_TO_REGION_CODE
except ImportError:
    sys.exit("Missing dependency: pip install phonenumbers")

try:
    import pycountry
except ImportError:
    sys.exit("Missing dependency: pip install pycountry")


NEW_COLUMN_NAME = "Derived Country (Dial Code)"

# Region codes returned by phonenumbers are ISO 3166-1 alpha-2 (e.g. "US",
# "IN", "GB"). Where more than one country shares a calling code, phonenumbers
# lists the "main" country first (its own convention) -- we take that one.
_REGION_TO_COUNTRY_NAME_CACHE = {}


def region_code_to_country_name(region_code: str) -> str:
    """ISO alpha-2 region code -> full country name via pycountry."""
    if not region_code or region_code == "001":  # "001" = world/unknown in libphonenumber
        return ""
    if region_code in _REGION_TO_COUNTRY_NAME_CACHE:
        return _REGION_TO_COUNTRY_NAME_CACHE[region_code]
    try:
        country = pycountry.countries.get(alpha_2=region_code)
        name = country.name if country else ""
    except LookupError:
        name = ""
    _REGION_TO_COUNTRY_NAME_CACHE[region_code] = name
    return name


_DIAL_CODE_CACHE = {}


def dial_code_to_country(raw_dial_code: str) -> str:
    """
    '+91' -> 'India', '+1' -> 'United States', '-' / '' / None -> ''.
    Unrecognized codes return '' too (so the column reads blank, not an error).
    """
    if not raw_dial_code:
        return ""
    code = raw_dial_code.strip()
    if code in ("", "-", "--", "N/A", "NA", "null", "None"):
        return ""
    code = code.lstrip("+").strip()
    if not code.isdigit():
        # Some export rows are malformed (e.g. "+7 7", stray spaces/duplicated
        # digits). Fall back to the leading run of digits rather than
        # discarding the row outright.
        match = re.match(r"^\d+", code)
        if not match:
            return ""
        code = match.group(0)
    if code in _DIAL_CODE_CACHE:
        return _DIAL_CODE_CACHE[code]

    country_code_int = int(code)
    region_codes = COUNTRY_CODE_TO_REGION_CODE.get(country_code_int, [])
    result = region_code_to_country_name(region_codes[0]) if region_codes else ""
    _DIAL_CODE_CACHE[code] = result
    return result


def process_csv(input_path: Path, output_path: Path, dial_code_column: str, encoding: str):
    with open(input_path, newline="", encoding=encoding) as f_in:
        # The Edmingle export has one junk title line ("Student's Export")
        # above the real header row. Detect and preserve it separately so
        # it can be written back unchanged.
        first_line = f_in.readline()
        looks_like_title_row = "," not in first_line.strip().strip('"')
        if not looks_like_title_row:
            f_in.seek(0)
            title_line = None
        else:
            title_line = first_line

        reader = csv.DictReader(f_in)
        if reader.fieldnames is None:
            sys.exit("Could not read a header row from the input CSV.")
        if dial_code_column not in reader.fieldnames:
            sys.exit(
                f"Column '{dial_code_column}' not found in the CSV header.\n"
                f"Available columns: {reader.fieldnames}"
            )

        fieldnames = list(reader.fieldnames)
        if NEW_COLUMN_NAME in fieldnames:
            fieldnames.remove(NEW_COLUMN_NAME)
        fieldnames.append(NEW_COLUMN_NAME)

        rows = []
        total = 0
        derived = 0
        blank_or_unrecognized = 0
        for row in reader:
            total += 1
            country = dial_code_to_country(row.get(dial_code_column, ""))
            row[NEW_COLUMN_NAME] = country
            if country:
                derived += 1
            else:
                blank_or_unrecognized += 1
            rows.append(row)

    with open(output_path, "w", newline="", encoding="utf-8") as f_out:
        if title_line:
            f_out.write(title_line if title_line.endswith("\n") else title_line + "\n")
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Read {total} rows from {input_path}")
    print(f"  -> {derived} rows got a derived country from '{dial_code_column}'")
    print(f"  -> {blank_or_unrecognized} rows left blank (missing/'-'/unrecognized dial code)")
    print(f"Wrote {output_path} with new column '{NEW_COLUMN_NAME}'")


DEFAULT_INPUT_GLOB = "Student-Export*.csv"


def find_default_input() -> Path:
    """When --input isn't given, pick the most recently modified file
    matching Student-Export*.csv in ../input/ (this pipeline's input
    folder, not the current working directory) -- lets this script be
    re-run as-is every time a fresh export lands there."""
    candidates = [p for p in INPUT_DIR.glob(DEFAULT_INPUT_GLOB) if "_with_country" not in p.name]
    if not candidates:
        sys.exit(
            f"No --input given and no file matching '{DEFAULT_INPUT_GLOB}' found in {INPUT_DIR}.\n"
            f"Either pass --input explicitly, or drop the export CSV into {INPUT_DIR}."
        )
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    if len(candidates) > 1:
        print(f"Multiple exports found, using the most recent: {candidates[0].name}")
    return candidates[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=None,
                         help=f"Path to the input student export CSV (default: newest '{DEFAULT_INPUT_GLOB}' in the current folder)")
    parser.add_argument("--output", default=None, help="Path to write the output CSV (default: <input>_with_country.csv)")
    parser.add_argument("--dial-code-column", default="Contact Number Dial Code",
                         help="Name of the dial-code column (default: 'Contact Number Dial Code')")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input file encoding (default: utf-8-sig, handles Excel BOM)")
    args = parser.parse_args()

    input_path = Path(args.input) if args.input else find_default_input()
    if not input_path.exists():
        sys.exit(f"Input file not found: {input_path}")

    if args.output:
        output_path = Path(args.output)
    else:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / f"{input_path.stem}_with_country{input_path.suffix}"

    process_csv(input_path, output_path, args.dial_code_column, args.encoding)


if __name__ == "__main__":
    main()
