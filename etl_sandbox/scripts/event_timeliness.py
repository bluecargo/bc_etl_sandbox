from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

spark.read.parquet("data/source-tables/containers_containerevent/*.parquet").drop("data").createOrReplaceTempView("events")
spark.read.parquet("data/source-tables/scraping_scrapingsession/*.parquet").filter(
        (F.col("status") == "COMPLETED") | (F.col("status") == "NO_DATA_FOUND")).createOrReplaceTempView("sessions")
spark.read.parquet("data/source-tables/containers_shipment_transactions/*.parquet").createOrReplaceTempView("shipment_transactions")

spark.sql(
    """
    SELECT
        id 
        , created_at
        , transaction_id
        , CASE
            WHEN source == 'shipping line' THEN 'SSL'
            WHEN source IN ('individual gate', 'bulk gate') THEN 'GATE'
            WHEN source == 'pickup' THEN 'PICKUP'
        END AS scope
        , CASE 
            WHEN source == 'shipping line' THEN shipping_line_id
            ELSE terminal_id
        END AS target_id
    FROM events
    --WHERE created_at >= current_date - interval 1 year
    """
).createOrReplaceTempView("sub_events")

spark.sql(
    """
    SELECT
        id
        , created_at
        , context
        , status
        , CASE
            WHEN terminal_id IS NOT NULL THEN 'terminal'
            ELSE 'shipping_line'
        END AS target_type
        , COALESCE(terminal_id, shipping_line_id) AS target_id
        , CASE 
            WHEN transaction_id IS NOT NULL THEN 'transaction'
            ELSE 'shipment'
        END AS entity_type
        , COALESCE(transaction_id, shipment_id) AS entity_id
    FROM sessions
    """
).createOrReplaceTempView("sessions")

spark.sql(
    """
    SELECT
        s.created_at
        , s.id
        , s.context
        , s.status
        , s.target_id
        , entity_id AS transaction_id
        , NULL AS derived_from_shipment
    FROM sessions s
    WHERE entity_type = 'transaction'
    """
).createOrReplaceTempView("transaction_sessions")

spark.sql(
    """
    SELECT
        s.created_at
        , s.id
        , s.context
        , s.status
        , s.target_id
        , st.containertransaction_id AS transaction_id
        , s.entity_id AS derived_from_shipment
    FROM sessions s
    LEFT JOIN shipment_transactions st
        ON st.shipment_id = s.entity_id
    WHERE s.entity_type = 'shipment'
    """
).createOrReplaceTempView("shipment_sessions")

spark.sql(
    """
    SELECT t.* FROM transaction_sessions t
    UNION ALL 
    SELECT s.* FROM shipment_sessions s
    """
).createOrReplaceTempView("sessions")

spark.sql(
    """
    SELECT
        *
        , CASE WHEN status = 'COMPLETED' THEN created_at END AS origin_candidate_created_at
        , CASE WHEN status IN ('NO_DATA_FOUND', 'COMPLETED') THEN created_at END AS previous_candidate_created_at
    FROM sessions
    """
).createOrReplaceTempView("sessions")

spark.sql(
    """
    SELECT
        e.id AS event_id
        , e.created_at AS event_created_at
        , s.id AS session_id
        , s.created_at AS session_created_at
        , s.origin_candidate_created_at AS origin_candidate_created_at
        , s.previous_candidate_created_at AS previous_candidate_created_at
        , MAX_BY(s.id, s.origin_candidate_created_at) OVER (PARTITION BY e.id) AS origin_session_id
        , MAX(s.origin_candidate_created_at) OVER (PARTITION BY e.id) AS origin_session_created_at
    FROM sub_events e
    LEFT JOIN sessions s
        ON (s.transaction_id = e.transaction_id AND s.target_id = e.target_id AND e.scope = s.context)
    WHERE s.created_at <= e.created_at
    """
).createOrReplaceTempView("origin_sessions")

spark.sql(
    """
    SELECT
        event_id
        , event_created_at
        , session_id
        , session_created_at
        , origin_session_id
        , origin_session_created_at
        , MAX_BY(
            event_id, 
            CASE 
                WHEN origin_session_created_at IS NULL THEN previous_candidate_created_at
                WHEN previous_candidate_created_at < origin_session_created_at THEN previous_candidate_created_at
            END
        ) OVER (PARTITION BY event_id) AS previous_session_id
        , MAX(
            CASE 
                WHEN origin_session_created_at IS NULL THEN previous_candidate_created_at
                WHEN previous_candidate_created_at < origin_session_created_at THEN previous_candidate_created_at
            END
        ) OVER (PARTITION BY event_id) AS previous_session_created_at
    FROM origin_sessions
    """
).createOrReplaceTempView("origin_and_previous_sessions")

spark.sql(
    """
    SELECT
        event_id
        , FIRST(origin_session_id IGNORE NULLS) AS origin_session_id
        , FIRST(origin_session_created_at IGNORE NULLS) AS origin_session_created_at
        , FIRST(previous_session_id IGNORE NULLS) AS previous_session_id
        , FIRST(previous_session_created_at IGNORE NULLS) AS previous_session_created_at
    FROM origin_and_previous_sessions
    GROUP BY event_id
    """
).createOrReplaceTempView("event_boundaries")

spark.sql(
    """
    SELECT
        e.*, 
        eb.origin_session_id,
        eb.origin_session_created_at,
        eb.previous_session_id,
        eb.previous_session_created_at
    FROM events e
    LEFT JOIN event_boundaries eb
        ON eb.event_id = e.id
    """
).createOrReplaceTempView("event_timeliness")

spark.sql("SELECT * FROM event_timeliness").write.mode("overwrite").parquet("data/event_timeliness")
