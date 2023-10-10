from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

spark.read.parquet("data/source-tables/scraping_scrapingsession/*.parquet").createOrReplaceTempView("sessions")
spark.read.parquet("data/source-tables/search_transactionrefreshschedule/*.parquet").createOrReplaceTempView("transaction_schedules")
spark.read.parquet("data/source-tables/search_shipmentrefreshschedule/*.parquet").createOrReplaceTempView("shipment_schedules")
spark.read.parquet("data/source-tables/containers_shipment_transactions/*.parquet").createOrReplaceTempView("shipment_transactions")

spark.sql(
    """
    SELECT
        id
        , created_at
        , target_id
        , target_type
        , search_type
        , status
        , transaction_id
        , terminated_at
        , termination_code
        , context
        , NULL AS derived_from_shipment
        , _export_timestamp
    FROM transaction_schedules
    """
).createOrReplaceTempView("transaction_schedules")

spark.sql(
    """
    SELECT
        s.id
        , s.created_at
        , s.target_id
        , s.target_type
        , s.search_type
        , s.status
        , st.containertransaction_id AS transaction_id
        , s.terminated_at
        , s.termination_code
        , NULL AS context
        , s.shipment_id AS derived_from_shipment
        , s._export_timestamp
    FROM shipment_schedules s
    LEFT JOIN shipment_transactions st
        ON st.shipment_id = s.shipment_id
    """
).createOrReplaceTempView("shipment_schedules")

spark.sql(
    """
    SELECT * FROM transaction_schedules t
    UNION ALL
    SELECT * FROM shipment_schedules s
    """
).createOrReplaceTempView("schedules")

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
        sche.transaction_id
        , sche.target_type
        , sche.target_id
        , sche.created_at
        , sche.terminated_at
        , sche.termination_code
        , sche.context
        , sche.id AS schedule_id
        , s.id AS session_id
        , s.created_at AS session_created_at
        , s.status AS session_status
        , s.context AS session_context
        , s.target_id AS session_target_id
        , CASE 
            WHEN s.created_at < sche.created_at THEN 'early'
            WHEN s.created_at > COALESCE(sche.terminated_at, sche._export_timestamp) THEN 'late'
            WHEN s.created_at BETWEEN sche.created_at AND COALESCE(sche.terminated_at, sche._export_timestamp) THEN 'in_scope'
        END AS punctuality
    FROM schedules sche
    LEFT JOIN sessions s
        ON (s.transaction_id = sche.transaction_id AND UPPER(s.context) = UPPER(sche.target_type) AND s.target_id = sche.target_id)
    """
).createOrReplaceTempView("search_engine")

spark.sql(
    """
    SELECT
        transaction_id
        , target_type
        , target_id
        , context
        , punctuality
        , session_status
        , COUNT(DISTINCT schedule_id) AS schedule_count
        , COUNT(DISTINCT session_id) AS session_count
        , MIN(session_created_at) AS first_session_created_at
        , MAX(session_created_at) AS last_session_created_at
    FROM search_engine
    GROUP BY transaction_id, target_type, target_id, context, punctuality, session_status
    """
).createOrReplaceTempView("search_engine_facts")

spark.sql("SELECT * FROM search_engine").write.mode("overwrite").parquet("data/search_engine")
spark.sql("SELECT * FROM search_engine_facts").write.mode("overwrite").parquet("data/search_engine_facts")
