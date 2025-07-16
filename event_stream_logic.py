from pyspark.sql.functions import col, lit, lower, coalesce, when
from pyspark.sql import Column

# ────────────────────────────────────────────────────────────
# 1.  Generic predicates shared by multiple rules
# ────────────────────────────────────────────────────────────

is_click        = (col("event_type") == "click")
is_render       = (col("event_type") == "render")

# Fix #1 – make the comparisons to "default" and "auto" case-insensitive
not_auto_click  = lower(coalesce(col("event_value"), lit("UNKNOWN"))) != "auto"

valid_click: Column = (
    is_click &
    ~(
        (col("event_subtype") == "all-apps") &
        (lower(col("event_value")) == "default")  # ← LOWER for case-insensitive check
    ) &
    not_auto_click
)

valid_render: Column = (
    is_render &
    (coalesce(col("event_context_sequence"), lit("true")) != "false")
)

valid_evt: Column = valid_click | valid_render

# ────────────────────────────────────────────────────────────
# 2.  Individual event-stream expressions
# ────────────────────────────────────────────────────────────

# Fix #2 – CCD__LEARN_TUTORIAL : only Apps / (tutorial | resources-tutorials)
learn_tutorial_expr: Column = when(
    valid_evt &
    (col("event_workflow") == "Apps") &
    col("event_subtype").isin("tutorial", "resources-tutorials"),
    lit("CCD__LEARN_TUTORIAL")
)

# Fix #3 – CCD__TUTORIALS : clicks only + four sub-clauses

tutorials_expr: Column = when(
    valid_click &  # clicks only
    (
        # a) Apps / resources-tutorials / Resources
        (
            (col("event_workflow")   == "Apps") &
            (col("event_subcategory") == "Apps") &
            (col("event_subtype")     == "resources-tutorials") &
            (col("content_category")  == "Resources")
        )
        |
        # b) Tutorials cards inside Apps
        (
            col("event_subtype").isin("tutorial", "view-more") &
            (col("content_type") == "Tutorials")
        )
        |
        # c) Discover hybridRowsData preference / viewmore
        (
            (col("event_workflow") == "Discover") &
            col("event_subtype").isin("preference", "viewmore") &
            (col("content_category") == "hybridRowsData")
        )
        |
        # d) Discover checklistData renders
        (
            (col("event_workflow") == "Discover") &
            (col("content_category") == "checklistData")
        )
    ),
    lit("CCD__TUTORIALS")
)