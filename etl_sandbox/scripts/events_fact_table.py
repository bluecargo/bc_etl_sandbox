from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

spark.read.parquet("data/containers_containerevent/*.parquet").drop("data").createOrReplaceTempView("events")
spark.read.parquet("data/scraping_scrapingsession/*.parquet").filter(
        (F.col("status") == "COMPLETED") | (F.col("status") == "NO_DATA_FOUND")).createOrReplaceTempView("sessions")
spark.read.parquet("data/containers_shipment_transactions/*.parquet").createOrReplaceTempView("shipment_transactions")

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
    WHERE created_at >= current_date - interval 1 year
    """
).createOrReplaceTempView("sub_events")

spark.sql(
    """
    SELECT
        created_at
        , id
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

spark.sql("SELECT * FROM sessions WHERE entity_type = 'transaction'").createOrReplaceTempView("transaction_sessions")
spark.sql(
    """
    SELECT 
        s.created_at
        , s.id 
        , s.context
        , s.status
        , s.target_type
        , s.target_id
        , 'transaction' AS entity_type
        , st.containertransaction_id AS entity_id
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
).drop("entity_type", "target_type").createOrReplaceTempView("sessions")

spark.sql(
    """
    SELECT
        e.id AS event_id
        , e.created_at AS event_created_at
        , s.id AS session_id
        , s.created_at AS session_created_at
        , NTH_VALUE(CASE WHEN s.status == 'COMPLETED' THEN s.created_at END, 1) OVER (PARTITION BY e.id ORDER BY s.created_at DESC) AS origin_session_created_at
        , NTH_VALUE(s.created_at, 2) OVER (PARTITION BY e.id ORDER BY s.created_at DESC) AS previous_session_created_at
    FROM sub_events e
    LEFT JOIN sessions s
        ON (s.entity_id = e.transaction_id AND s.target_id = e.target_id AND e.scope = s.context)
    WHERE s.created_at <= e.created_at
    """
).createOrReplaceTempView("sessions_per_event")

spark.sql(
    """
    SELECT
        event_id
        , CASE WHEN origin_session_created_at == session_created_at THEN session_id END AS origin_session_id
        , CASE WHEN origin_session_created_at == session_created_at THEN session_created_at END AS origin_session_created_at
        , CASE WHEN previous_session_created_at == session_created_at THEN session_id END AS previous_session_id
        , CASE WHEN previous_session_created_at == session_created_at THEN session_created_at END AS previous_session_created_at
    FROM sessions_per_event
    WHERE session_created_at = origin_session_created_at OR session_created_at = previous_session_created_at
    """
).createOrReplaceTempView("sessions_per_event")

spark.sql(
    """
    SELECT
        event_id
        , FIRST(origin_session_id IGNORE NULLS) AS origin_session_id
        , FIRST(origin_session_created_at IGNORE NULLS) AS origin_session_created_at
        , FIRST(previous_session_id IGNORE NULLS) AS previous_session_id
        , FIRST(previous_session_created_at IGNORE NULLS) AS previous_session_created_at
    FROM sessions_per_event
    GROUP BY event_id
    """
).createOrReplaceTempView("event_info")

facts = spark.sql(
    """
    SELECT 
        e.*,
        ei.origin_session_id,
        ei.origin_session_created_at,
        ei.previous_session_id,
        ei.previous_session_created_at
    FROM events e
    LEFT JOIN event_info ei
        ON ei.event_id = e.id
    """
)

facts.write.mode("overwrite").parquet("events_facts")
