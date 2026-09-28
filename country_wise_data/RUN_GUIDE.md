# country_wise_data — Run Guide

Works out each learner's country and saves it in one file. It runs in 3 stages: (1) Edmingle's own location guess, (2) a guess from the phone number, (3) merge the two. Full technical detail: [COUNTRY_WISE_DATA.md](COUNTRY_WISE_DATA.md).

## Before you start
- Make sure `credentials.yaml` (shared) has a working key — needed for Stage 1 only.
- Before Stage 2, download a fresh Student Export from the Edmingle admin panel and put it in `input/` as `Student-Export*.csv`.
- Always run the 3 stages **in order** — each one needs the file the last one made.
- Stage 1 always pulls everything fresh — there's no "continue where it left off," it just takes 5–15 minutes.
- Only run one pipeline at a time.

## Steps
1. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the folder: `cd /home/projectdev/ela_datasets/country_wise_data/scripts`
3. Run all three, in order:
   ```
   python3 ip_driven_country_data.py --config ip_driven_country_data_config.json   # Stage 1
   python3 dial_code_to_country.py                                                 # Stage 2
   python3 merge_country_data.py                                                   # Stage 3
   ```

## How to tell it worked
- `output/Student-Export_with_country.csv` and `output/merged_country_data.csv` are freshly rewritten.
- The merged file should have the same number of rows as the Student Export you started with.

## If something goes wrong
- **Stage 1 fails partway** — just run it again from the start; nothing is kept from a failed run.
- **Stage 2 says "No --input given"** — you forgot to put a Student Export file in `input/`.
- **Stage 3 says a file is "not found"** — you skipped Stage 1 or 2; run them first.

## Running it in the background (Stage 1 is the long one)
1. `tmux new -s country_wise_data`
2. `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. `cd /home/projectdev/ela_datasets/country_wise_data/scripts`
4. Run the 3 commands from Step 3 above, one after another.
5. Detach and leave it running: press `Ctrl+B`, then `D`.
6. Come back later: `tmux attach -t country_wise_data`
