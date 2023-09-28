from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

spark.read.parquet("data/source-tables/scraping_scrapingsession/*.parquet").createOrReplaceTempView("sessions")
spark.read.parquet("data/source-tables/containers_shipment_transactions/*.parquet").createOrReplaceTempView("shipment_transactions")

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
        , st.shipment_id AS derived_from_shipment
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

spark.sql("SELECT * FROM sessions").write.mode("overwrite").parquet("data/target-tables/sessions")
