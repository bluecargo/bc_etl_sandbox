from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

spark.read.parquet("data/scraping_scrapingsession/*.parquet").createOrReplaceTempView("sessions")
spark.read.parquet("data/containers_shipment_transactions/*.parquet").createOrReplaceTempView("shipment_transactions")

spark.sql(
    """
    SELECT 
        id
        , created_at
        , context
        , CASE WHEN status = 'COMPLETED' THEN id END AS completed_id
        , CASE WHEN status = 'COMPLETED' THEN created_at END AS completed_created_at
        , CASE WHEN status = 'NO_DATA_FOUND' THEN id END AS no_data_found_id
        , CASE WHEN status = 'NO_DATA_FOUND' THEN created_at END AS no_data_found_created_at
        , CASE WHEN status = 'IN_PROGRESS' THEN id END AS in_progress_id
        , CASE WHEN status = 'IN_PROGRESS' THEN created_at END AS in_progress_created_at
        , CASE WHEN status = 'ABORTED' THEN id END AS aborted_id
        , CASE WHEN status = 'ABORTED' THEN created_at END AS aborted_created_at
        , CASE WHEN status = 'FAILED' THEN id END AS failed_id
        , CASE WHEN status = 'FAILED' THEN created_at END AS failed_created_at
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
