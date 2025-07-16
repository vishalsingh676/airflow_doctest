from pyspark.sql.functions import expr
from pyspark.sql import DataFrame

# Full SQL CASE-logic copied verbatim from the original Hive query, wrapped
# in an ARRAY() so that it returns exactly the same event_stream_array as
# the reference notebook.  Nothing in this string may be modified without
# re-validating the counts against the SQL job.

_EVENT_STREAM_ARRAY_SQL = """
array(
    --All Valid Clicks
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value, 'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN (
                         event_subcategory  = 'Cloud Sync'
                         AND event_subtype  = 'icon'
                         AND event_type     = 'click'
                         AND event_workflow = 'Cloud Sync'
                         AND event_date    >= '2021-12-10'
                         AND event_date    <= '2021-12-25'
                      )
                      OR (
                         event_subcategory  = 'Appearance'
                         AND event_subtype  = 'mode'
                         AND event_workflow = 'Preferences'
                         AND event_value IN ('system:dark','system:light')
                      )
                 THEN NULL
                 ELSE 'CCD__CLICKS'
             END
        ELSE NULL
    END,

    --Opt-out Events
    CASE
        WHEN event_workflow = 'Privacy'
             AND event_subcategory = 'Opt-out'
             AND event_type = 'user'
             AND event_subtype = 'opt-out'
        THEN 'CCD__OPTOUT'
        ELSE NULL
    END,

    --Intent Events
    CASE
        WHEN event_date >= '2020-03-28'
             AND event_subcategory LIKE 'UserSignals%'
             AND event_subtype IN ('continue','preference')
        THEN 'CCD__INTENT'
        ELSE NULL
    END,

    --CCD Update Events
    CASE
        WHEN event_workflow = 'Bootstrap'
             AND event_subcategory = 'CCD Update'
             AND event_type = 'install'
             AND event_subtype = 'end'
        THEN 'CCD__CCD_UPDATE'
        ELSE NULL
    END,

    --Discover Events
    CASE
        WHEN (
                 event_type = 'click'
                 AND CASE
                         WHEN event_subtype = 'all-apps'
                         THEN CASE
                                 WHEN event_value = 'default' THEN FALSE
                                 ELSE TRUE
                              END
                         ELSE TRUE
                     END
                 AND COALESCE(event_value,'UNKNOWN') <> 'auto'
             )
             OR (
                 event_type = 'render'
                 AND COALESCE(event_context_sequence,'true') <> 'false'
             )
        THEN CASE
                 WHEN event_workflow = 'Discover'
                 THEN 'CCD__DISCOVER'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Saw CCD App Panel Events
    CASE
        WHEN event_type = 'render'
             AND COALESCE(event_context_sequence,'true') <> 'false'
        THEN CASE
                 WHEN event_workflow = 'Main'
                      AND event_subcategory = 'Apps'
                      AND event_subtype = 'page'
                 THEN 'CCD__MAIN_APP_PANEL'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Targeted Campaigns
    CASE
        WHEN exp_campaign_id LIKE '%2020-02-NUJ-M2%'
             OR exp_campaign_id LIKE '%2020-06-NUJ-M3-Photo%'
             OR exp_campaign_id LIKE '%2020-05-NUJ-M3%'
             OR exp_campaign_id LIKE '%2020-08-NUJ-M3-AllApps%'
        THEN 'CCD__TARGETED_CAMPAIGN'
        ELSE NULL
    END,

    --Banners
    CASE
        WHEN (
                 event_type = 'click'
                 AND CASE
                         WHEN event_subtype = 'all-apps'
                         THEN CASE
                                 WHEN event_value = 'default' THEN FALSE
                                 ELSE TRUE
                              END
                         ELSE TRUE
                     END
                 AND COALESCE(event_value,'UNKNOWN') <> 'auto'
             )
             OR (
                 event_type = 'render'
                 AND COALESCE(event_context_sequence,'true') <> 'false'
             )
        THEN CASE
                 WHEN event_subtype = 'banner'
                 THEN 'CCD__BANNER'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Learn/Tutorial
    CASE
        WHEN (
                 event_type = 'click'
                 AND CASE
                         WHEN event_subtype = 'all-apps'
                         THEN CASE
                                 WHEN event_value = 'default' THEN FALSE
                                 ELSE TRUE
                              END
                         ELSE TRUE
                     END
                 AND COALESCE(event_value,'UNKNOWN') <> 'auto'
             )
             OR (
                 event_type = 'render'
                 AND COALESCE(event_context_sequence,'true') <> 'false'
             )
        THEN CASE
                 WHEN event_workflow = 'Apps'
                      AND event_subtype IN ('tutorial','resources-tutorials')
                 THEN 'CCD__LEARN_TUTORIAL'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Fonts
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN (
                         event_workflow = 'Apps'
                         AND event_subtype = 'fonts-panel'
                      )
                      OR (
                         event_workflow = 'Market Place'
                         AND event_subcategory = 'Navigation'
                         AND event_subtype = 'fonts'
                      )
                      OR (
                         content_name = 'Adobe Fonts'
                         AND event_subtype = 'launch'
                      )
                 THEN 'CCD__FONTS'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Behance
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN (
                         event_workflow = 'Apps'
                         AND event_category = 'DESKTOP'
                         AND event_subcategory = 'Apps'
                         AND event_subtype = 'resources-behance'
                         AND content_category = 'Resources'
                      )
                      OR (
                         event_workflow = 'Apps'
                         AND event_subtype = 'launch'
                         AND content_id = 'FFC_behance'
                         AND content_name = 'Behance'
                      )
                      OR (
                         event_workflow = 'Discover'
                         AND event_subtype IN ('card','viewmore')
                         AND content_category = 'behanceProjectsData'
                      )
                      OR (
                         event_workflow = 'Apps'
                         AND event_subtype IN ('project','view-more')
                         AND content_type = 'Behance Projects'
                      )
                 THEN 'CCD__BEHANCE'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Community
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN (
                         event_subcategory = 'Apps'
                         AND event_subtype = 'support-community-resource'
                         AND content_category = 'Resources'
                      )
                      OR (
                         event_workflow = 'Discover'
                         AND event_subtype IN ('card','viewmore')
                         AND content_category = 'communityConversations'
                      )
                      OR (
                         event_workflow = 'Apps'
                         AND event_subcategory LIKE 'PDP%'
                         AND event_subtype = 'help-support-link'
                         AND event_value = 'Community & Forum'
                      )
                 THEN 'CCD__COMMUNITY'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Stock
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN (
                         event_subtype = 'banner'
                         AND LOWER(content_id) LIKE '%stock%'
                      )
                      OR (
                         event_workflow = 'Apps'
                         AND event_subcategory = 'Apps'
                         AND event_subtype = 'resources-stock'
                      )
                      OR (
                         event_workflow = 'Market Place'
                         AND event_subcategory = 'Navigation'
                         AND event_subtype = 'stock'
                      )
                      OR (
                         event_subtype = 'launch'
                         AND content_id = 'FFC_stock'
                      )
                 THEN 'CCD__STOCK'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Plugins
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN event_subcategory LIKE 'Plugin%'
                      OR event_subcategory LIKE '%Plugins'
                 THEN 'CCD__PLUGINS'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Account Management
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN event_subtype IN ('manage-account','sign-out','help','chat')
                 THEN 'CCD__ACCOUNT_MANAGEMENT'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Tutorials
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN (
                         event_workflow = 'Apps'
                         AND event_subcategory = 'Apps'
                         AND event_subtype = 'resources-tutorials'
                         AND content_category = 'Resources'
                      )
                      OR (
                         event_subtype IN ('tutorial','view-more')
                         AND content_type = 'Tutorials'
                      )
                      OR (
                         event_workflow = 'Discover'
                         AND event_subtype IN ('preference','viewmore')
                         AND content_category = 'hybridRowsData'
                      )
                      OR (
                         event_workflow = 'Discover'
                         AND content_category = 'checklistData'
                      )
                 THEN 'CCD__TUTORIALS'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Portfolio
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN (
                         event_workflow = 'Apps'
                         AND event_subcategory = 'Apps'
                         AND event_subtype = 'portfolio'
                         AND content_category = 'Resources'
                      )
                      OR (
                         event_subtype = 'launch'
                         AND content_id = 'FFC_portfolio'
                         AND content_name = 'Portfolio'
                      )
                 THEN 'CCD__PORTFOLIO'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --Top Actions
    CASE
        WHEN event_type = 'click'
             AND CASE
                     WHEN event_subtype = 'all-apps'
                     THEN CASE
                             WHEN event_value = 'default' THEN FALSE
                             ELSE TRUE
                          END
                     ELSE TRUE
                 END
             AND COALESCE(event_value,'UNKNOWN') <> 'auto'
        THEN CASE
                 WHEN event_subtype IN ('open','update','launch','send-Link','install','uninstall','buy','buy-now','try','start-trial')
                 THEN 'CCD__TOP_ACTIONS'
                 ELSE NULL
             END
        ELSE NULL
    END,

    --CCD__MARKETPLACE
    CASE
        WHEN (
                event_workflow IN ('Market Place','Marketplace')
             )
             OR (
                event_workflow = 'Main'
                AND event_subcategory IN ('Market Place','Marketplace')
             )
        THEN 'CCD__MARKETPLACE'
        ELSE NULL
    END,

    --CCD__DEEP_LINK
    CASE
        WHEN event_workflow = 'Deep Link'
        THEN 'CCD__DEEP_LINK'
        ELSE NULL
    END,

    --CCD__HOME_NAV
    CASE
        WHEN event_workflow = 'Home'
             AND event_subcategory = 'Navigation'
        THEN 'CCD__HOME_NAV'
        ELSE NULL
    END,

    --CCD__MARKETING_PREF
    CASE
        WHEN LOWER(event_workflow) = 'preferences'
             AND LOWER(event_subtype) = 'marketing'
        THEN 'CCD__MARKETING_PREF'
        ELSE NULL
    END,

    --CCD__HOME_CARD
    CASE
        WHEN (
                 event_subcategory = 'Action items'
                 AND event_subtype = 'overflow-menu'
                 AND event_type = 'click'
                 AND event_workflow = 'Home'
             )
             OR (
                 event_subcategory = 'Action items'
                 AND event_subtype = 'card'
                 AND event_type = 'render'
                 AND event_workflow = 'Home'
             )
        THEN 'CCD__HOME_CARD'
        ELSE NULL
    END,

    --CCD__CHECKLIST
    CASE
        WHEN event_workflow = 'Get started'
             AND event_subcategory = 'Quick Start Guide'
             AND event_date >= '2023-04-28'
        THEN 'CCD__CHECKLIST'
        ELSE NULL
    END,

    --CCD__BETA_APPS
    CASE
        WHEN event_subcategory = 'beta-apps'
             AND event_subtype = 'app'
             AND event_type = 'render'
             AND event_date >= '2023-05-10'
        THEN 'CCD__BETA_APPS'
        ELSE NULL
    END,

    --CCD__ACTIVATION_MODAL
    CASE
        WHEN event_workflow = 'Main'
             AND event_subcategory = 'Whats New'
             AND event_subtype IN ('modal','button')
             AND event_date >= '2023-04-27'
             AND LOWER(event_type) IN ('render','click')
        THEN 'CCD__ACTIVATION_MODAL'
        ELSE NULL
    END,

    --CCD__OS_NOTIFICATION_STATUS
    CASE
        WHEN event_workflow = 'OS Notification'
             AND event_type = 'notificationSettings'
        THEN 'CCD__OS_NOTIFICATION_STATUS'
        ELSE NULL
    END,

    --CCD__DEBUG_INFO
    CASE
        WHEN event_workflow = 'Debug'
             AND event_subcategory = 'CCD Apps'
             AND event_subtype = 'log'
             AND event_type = 'info'
        THEN 'CCD__DEBUG_INFO'
        ELSE NULL
    END,

    --CCD__CUSTOM_CARD
    CASE
        WHEN event_subtype = 'custom-card'
        THEN 'CCD__CUSTOM_CARD'
        ELSE NULL
    END
)
"""

def add_event_stream_array(df: DataFrame) -> DataFrame:
    """Attach `event_stream_array` to *df* using the canonical SQL logic.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The DataFrame that already contains all base columns from
        tmp__edsna__ccd_data__*.  The column names must exactly match the
        ones used in the original Hive query.

    Returns
    -------
    DataFrame
        Same rows as *df* plus a new column `event_stream_array` that is
        byte-for-byte identical to the Hive implementation.
    """
    return df.withColumn("event_stream_array", expr(_EVENT_STREAM_ARRAY_SQL))