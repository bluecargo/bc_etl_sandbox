from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

spark.read.parquet("data/sessions/*.parquet").createOrReplaceTempView("sessions")
spark.read.parquet("data/source-tables/search_transactionrefreshschedule/*.parquet").createOrReplaceTempView("schedules")

spark.sql(
    """
    SELECT 
        id
        , created_at
        , transaction_id
        , derived_from_shipment
        , context
        , target_id
        , status
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
    FROM sessions
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
        , s.id AS session_id
        , s.created_at AS session_created_at
        , s.status AS session_status
        , s.context AS session_context
        , s.target_id AS session_target_id
        , CASE 
            WHEN s.created_at < sche.created_at THEN 'early'
            WHEN s.created_at > COALESCE(sche.terminated_at, sche._export_timestamp) THEN 'late'
            ELSE 'in_scope'
        END AS punctuality
    FROM schedules sche
    LEFT JOIN sessions s
        ON (s.transaction_id = sche.transaction_id AND UPPER(s.context) = UPPER(sche.target_type) AND s.target_id = sche.target_id)
    """
).createOrReplaceTempView("tmp")

spark.sql("SELECT * FROM tmp").write.mode("overwrite").parquet("data/search_efficiency")
