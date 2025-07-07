from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, split, upper, regexp_replace, coalesce, array, explode, collect_set, concat

# -----------------------------------------------------------------------------
# Configuration / parameters. Fill these in from your scheduler / DAG context.
# -----------------------------------------------------------------------------
days_ago_6_date_str = "2024-06-01"  # example, REPLACE
end_date_str        = "2024-06-07"  # example, REPLACE

# Derive the date ints if you need them downstream (same pattern as original)
# days_ago_6_date_int = int(days_ago_6_date_str.replace("-", ""))
# end_date_int        = int(end_date_str.replace("-", ""))

spark = SparkSession.builder.getOrCreate()

# -----------------------------------------------------------------------------
# 1.  Source data
# -----------------------------------------------------------------------------
df = spark.table("edsna__ccd_events_view")

# Filter to the desired dates first
# (the >='2019-10-01' lower-bound is preserved from the SQL)
df = df.filter(
    (col("event_date") >= lit("2019-10-01")) &
    col("event_date").isin(days_ago_6_date_str, end_date_str)
)

# -----------------------------------------------------------------------------
# 2.  Basic column transformations
# -----------------------------------------------------------------------------

df = df.withColumn("profile_guid", upper(split(col("member_guid"), "@")[0])) \
       .withColumn("event_timestamp", col("dts").cast("timestamp"))

# -----------------------------------------------------------------------------
# 3.  Build event_stream_array  (UPDATED IMPLEMENTATION)
# -----------------------------------------------------------------------------

event_stream_array = array(
    # ───────────────────────────────────────────────────────────────────────────
    #  a) CCD__CCD_USER (always present, literal)
    # ───────────────────────────────────────────────────────────────────────────
    lit("CCD__CCD_USER"),

    # ───────────────────────────────────────────────────────────────────────────
    #  b) CCD__CCD_ACTIVE_USER  (unchanged logic)
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "click") &
        (
            when(col("event_subtype") == "all-apps",
                 when(col("event_value") == "default", lit(False)).otherwise(lit(True))
            ).otherwise(lit(True))
        ) &
        (coalesce(col("event_value"), lit("UNKNOWN")) != "auto"),
        when(
            (
                (col("event_subcategory") == "Cloud Sync") &
                (col("event_subtype") == "icon") &
                (col("event_type") == "click") &
                (col("event_workflow") == "Cloud Sync") &
                (col("event_date") >= lit("2021-12-10")) &
                (col("event_date") <= lit("2021-12-25"))
            ) |
            (
                (col("event_subcategory") == "Appearance") &
                (col("event_subtype") == "mode") &
                (col("event_type") == "click") &
                (col("event_workflow") == "Preferences") &
                (col("event_value").isin("system:dark", "system:light"))
            ),
            lit(None)
        ).otherwise("CCD__CCD_ACTIVE_USER")
    ).otherwise(lit(None)),

    # ───────────────────────────────────────────────────────────────────────────
    #  c) CCD__CCD_ACTIVE_USER_WORKFLOW  (Update / Open / Launch / Install / …)
    #      (logic identical to original, untouched)
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "click") &
        (coalesce(col("event_value"), lit("UNKNOWN")) != "auto"),
        when(
            (col("event_workflow") == "Apps") &
            (col("content_category").isin("desktop", "web", "mobile")) &
            (col("event_subtype") == "update"),
            lit("CCD__CCD_ACTIVE_USER_WORKFLOW:::UPDATE")
        ).when(
            (col("event_workflow") == "Apps") &
            col("content_category").isNull() &
            col("event_subcategory").isNull() &
            (col("event_subtype") == "update-all"),
            lit("CCD__CCD_ACTIVE_USER_WORKFLOW:::UPDATE")
        ).when(
            (col("event_workflow") == "Apps") &
            (col("content_category").isin("desktop", "web", "mobile")) &
            (col("event_subtype") == "open"),
            lit("CCD__CCD_ACTIVE_USER_WORKFLOW:::OPEN")
        ).when(
            (col("event_workflow") == "Apps") &
            (col("content_category").isin("desktop", "web", "mobile")) &
            (col("event_subtype") == "launch"),
            lit("CCD__CCD_ACTIVE_USER_WORKFLOW:::LAUNCH")
        ).when(
            (col("event_workflow") == "Apps") &
            (col("event_subtype") == "install") &
            (
                col("content_category").isin("desktop", "web", "mobile") |
                (col("event_subcategory") == "Information:Version")
            ),
            lit("CCD__CCD_ACTIVE_USER_WORKFLOW:::INSTALL")
        ).when(
            (col("event_workflow").isin("Util Nav", "Preferences")) &
            (col("event_subtype") == "manage-account"),
            lit("CCD__CCD_ACTIVE_USER_WORKFLOW:::ACCOUNT_MANAGEMENT")
        ).otherwise(lit(None))
    ).otherwise(lit(None)),

    # ───────────────────────────────────────────────────────────────────────────
    #  d) CCD__CCD_ACTIVE_USER_WORKFLOW  – CATEGORIES / RESOURCES  (FIXED)
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "click") &
        (coalesce(col("event_value"), lit("UNKNOWN")) != "auto"),
        when(
            (col("event_workflow") == "Apps") &
            (col("content_category").isin("Categories", "Resources")) &
            (col("event_subcategory") == "Apps") &
            (col("event_subtype") != "all-apps"),
            # REPLACED concat_ws -> concat  +  ensured NULL-on-NULL
            concat(
                lit("CCD__CCD_ACTIVE_USER_WORKFLOW"), lit(":::"),
                upper(col("content_category"))
            )
        ).otherwise(lit(None))   # <-- added otherwise(None)
    ).otherwise(lit(None)),

    # ───────────────────────────────────────────────────────────────────────────
    #  e) CCD__CCD_ACTIVE_USER_WORKFLOW  – YOURWORK  (unchanged)
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "click") &
        (coalesce(col("event_value"), lit("UNKNOWN")) != "auto"),
        when(
            (col("event_workflow").isin("Your Work", "Files")) &
            (col("event_subtype") != "all-apps"),
            lit("CCD__CCD_ACTIVE_USER_WORKFLOW:::YOURWORK")
        ).otherwise(lit(None))
    ).otherwise(lit(None)),

    # ───────────────────────────────────────────────────────────────────────────
    #  f) CCD__CCD_BACKGROUND_TABAREA_USER_WORKFLOW  (unchanged – uses concat)
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "render") &
        (coalesce(col("event_context_sequence"), lit("true")) != "false") &
        col("event_workflow").isNotNull(),
        when(
            col("event_workflow").isin(
                "Apps", "Discover", "Gallery", "Home", "Marketplace", "Market Place", "OS Notification"
            ),
            concat(
                lit("CCD__CCD_BACKGROUND_TABAREA_USER_WORKFLOW"), lit(":::"),
                upper(regexp_replace(col("event_workflow"), " ", ""))
            )
        ).when(
            col("event_workflow").isin("Your Work", "Files"),
            lit("CCD__CCD_BACKGROUND_TABAREA_USER_WORKFLOW:::YOURWORK")
        ).when(
            (col("event_workflow") == "Main") & (col("event_subcategory") == "Quick Actions"),
            lit("CCD__CCD_BACKGROUND_TABAREA_USER_WORKFLOW:::QUICKACTIONS")
        ).otherwise(lit("CCD__CCD_BACKGROUND_TABAREA_USER_WORKFLOW:::OTHER"))
    ).otherwise(lit(None)),

    # ───────────────────────────────────────────────────────────────────────────
    #  g) CCD__CCD_ACTIVE_TABAREA_USER_WORKFLOW (unchanged)
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "click") &
        (
            when(col("event_subtype") == "all-apps",
                 when(col("event_value") == "default", lit(False)).otherwise(lit(True))
            ).otherwise(lit(True))
        ) &
        (coalesce(col("event_value"), lit("UNKNOWN")) != "auto"),
        when(col("event_workflow") == "Discover", lit("CCD__CCD_ACTIVE_TABAREA_USER_WORKFLOW:::DISCOVER"))
        .when(col("event_workflow") == "Apps", lit("CCD__CCD_ACTIVE_TABAREA_USER_WORKFLOW:::APPS"))
        .when(col("event_workflow").isin("Marketplace", "Market Place"), lit("CCD__CCD_ACTIVE_TABAREA_USER_WORKFLOW:::MARKETPLACE"))
        .when(col("event_workflow").isin("Your Work", "Files"), lit("CCD__CCD_ACTIVE_TABAREA_USER_WORKFLOW:::YOURWORK"))
        .otherwise(lit("CCD__CCD_ACTIVE_TABAREA_USER_WORKFLOW:::OTHER"))
    ).otherwise(lit(None)),

    # ───────────────────────────────────────────────────────────────────────────
    #  h) CCD__CCD_ACTIVE_SECTION_USER_WORKFLOW  (FIXED otherwise(None))
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "click") &
        (
            when(col("event_subtype") == "all-apps",
                 when(col("event_value") == "default", lit(False)).otherwise(lit(True))
            ).otherwise(lit(True))
        ) &
        (coalesce(col("event_value"), lit("UNKNOWN")) != "auto"),
        when(col("event_subcategory") == "All Apps",
             lit("CCD__CCD_ACTIVE_SECTION_USER_WORKFLOW:::ALLAPPS"))
        .when(col("event_subcategory").isin(
            "design", "ux-web", "illustration", "photography", "ar",
            "video-audio", "acrobat-pdf", "social-media"),
             lit("CCD__CCD_ACTIVE_SECTION_USER_WORKFLOW:::CATEGORY"))
        .when(col("event_subcategory").like("PDP%"),
             lit("CCD__CCD_ACTIVE_SECTION_USER_WORKFLOW:::PDP"))
        .when(col("event_subcategory") == "Update",
             lit("CCD__CCD_ACTIVE_SECTION_USER_WORKFLOW:::UPDATE"))
        .otherwise(lit(None))     # <-- added
    ).otherwise(lit(None)),

    # ───────────────────────────────────────────────────────────────────────────
    #  i) CCD__CCD_USER_APPS  ( newly added )
    # ───────────────────────────────────────────────────────────────────────────
    when(
        (col("event_type") == "click") &
        (col("source_version") >= "5.0") &
        (
            when(col("event_subtype") == "all-apps",
                 when(col("event_value") == "default", lit(False)).otherwise(lit(True))
            ).otherwise(lit(True))
        ) &
        (coalesce(col("event_value"), lit("UNKNOWN")) != "auto"),
        concat(
            lit("CCD__CCD_USER_APPS"), lit(":::"),
            lit("active"),             lit(":::"),
            coalesce(col("content_id"), lit("UNKNOWN"))
        )
    ).otherwise(
        concat(
            lit("CCD__CCD_USER_APPS"), lit(":::"),
            lit("other"),              lit(":::"),
            coalesce(col("content_id"), lit("UNKNOWN"))
        )
    ),

    # ───────────────────────────────────────────────────────────────────────────
    #  j) CCD__CAMPAIGN_USERS   (FIXED concat vs concat_ws)
    # ───────────────────────────────────────────────────────────────────────────
    when(
        col("exp_campaign_id").isNotNull(),
        concat(
            lit("CCD__CAMPAIGN_USERS"), lit(":::"),
            col("exp_campaign_id"),      lit(":::"),
            coalesce(col("exp_variation_id"), lit("UNKNOWN"))
        )
    ).otherwise(lit(None))
)

# -----------------------------------------------------------------------------
# 4.  attach array + explode + downstream logic
# -----------------------------------------------------------------------------

df = df.withColumn("event_stream_array", event_stream_array)

base_cols = [
    "profile_guid", "event_timestamp", "event_stream_array", "event_date"
]

df = df.select(*base_cols)

df = df.select("*", explode("event_stream_array").alias("event_stream"))

df = df.filter(col("event_stream").isNotNull() & col("profile_guid").isNotNull())

# -----------------------------------------------------------------------------
# 5.  Any additional aggregation & projection (omitted for brevity)
#     … replicate the remainder of your original pipeline …
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# 6.  Histogram – compare with Hive result
# -----------------------------------------------------------------------------

histogram_df = (
    df.select(split("event_stream", ":::")[0].alias("bucket"))
      .groupBy("bucket").count()
      .orderBy("bucket")
)

print("\nHistogram of first token in event_stream (PySpark output):")
histogram_df.show(100, False)

# For comparison you can load the Hive result (if materialised in a table)
# and run the same aggregation, then compare the two DataFrames.
# E.g.:
#
# hive_df = spark.table("xe_etl.tmp__edsna__high_value_events__<dates>_CCD_USERS")
# hive_hist = (hive_df.select(split("event_stream", ":::")[0].alias("bucket"))
#                         .groupBy("bucket").count()
#                         .orderBy("bucket"))
#
# print("\nHistogram from original Hive output:")
# hive_hist.show(100, False)
#
# ---------------------------------------------------------------------------------