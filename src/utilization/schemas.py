"""Explicit schemas for raw sources. Never let Spark infer a schema in production:
inference costs a full scan and silently changes types when the vendor changes theirs."""

from pyspark.sql.types import BooleanType, StringType, StructField, StructType

CORRUPT_RECORD = "_corrupt_record"

# Every field is read as a string; types are enforced in silver, where a bad value can be
# quarantined with a reason instead of failing the whole read.
TIMESHEET_RAW = StructType(
    [
        StructField("entry_id", StringType()),
        StructField("employee_id", StringType()),
        StructField("engagement_code", StringType()),
        StructField("work_date", StringType()),
        StructField("hours", StringType()),  # numbers and "7,5" strings both arrive here
        StructField("status", StringType()),
        StructField("updated_at", StringType()),  # ISO-8601 with mixed UTC offsets
        StructField("is_deleted", BooleanType()),  # added by the vendor in export 3
        StructField("activity_type", StringType()),  # added by the vendor in export 4
        StructField(CORRUPT_RECORD, StringType()),
    ]
)

TIMESHEET_FIELDS = [f.name for f in TIMESHEET_RAW.fields if f.name != CORRUPT_RECORD]

REFERENCE_FILES = {
    "employees": "employees.csv",
    "engagements": "engagements.csv",
    "rate_cards": "rate_cards.csv",
    "fx_rates": "fx_rates.csv",
}
