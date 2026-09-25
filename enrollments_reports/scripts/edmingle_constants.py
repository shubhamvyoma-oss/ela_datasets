"""
edmingle_constants.py

Shared constants used by every other edmingle_*.py module. Nothing in here
has side effects — just plain values.
"""

ENROLLMENT_PATH = "/reports/enrollment"  # appended to edmingle.base_url from credentials.yaml

DATE_FMT = "%d-%m-%Y"  # DD-MM-YYYY, the format Edmingle's API expects

# Column order for the output CSV. Matches the fields Edmingle returns in
# "studentlist" for report_details_type=3. If Edmingle adds/removes fields,
# update this list to match — unknown fields are dropped, missing fields are
# written as blank rather than crashing the run.
FIELDS = [
    "enrollment_id",
    "enrollment_day",
    "user_id",
    "name",
    "email",
    "contact_number",
    "contact_number_country_id",
    "state",
    "registration_number",
    "learner_type",
    "enrollment_mode",
    "enrollment_status",
    "bundle_id",
    "bundle_name",
    "batch_ids",
    "batches",
    "product_type",
    "product_type_label",
    "platform_type",
    "enrollment_expiration_date",
    "shipping_details_json",
    "preferred_categories",
]
