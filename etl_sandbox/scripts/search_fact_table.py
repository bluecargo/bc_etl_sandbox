from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, countDistinct, max_by, when, max as pmax, min as pmin, min_by

spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

spark.read.parquet("data/scraping_scrapingsession/*.parquet").createOrReplaceTempView("sessions")
spark.read.parquet("data/search_transactionrefreshschedule/*.parquet").createOrReplaceTempView("schedules")

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
        /**
        , CASE WHEN status = 'IN_PROGRESS' THEN id END AS in_progress_id
        , CASE WHEN status = 'IN_PROGRESS' THEN created_at END AS in_progress_created_at
        , CASE WHEN status = 'ABORTED' THEN id END AS aborted_id
        , CASE WHEN status = 'ABORTED' THEN created_at END AS aborted_created_at
        , CASE WHEN status = 'FAILED' THEN id END AS failed_id
        , CASE WHEN status = 'FAILED' THEN created_at END AS failed_created_at
        **/
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
        entity_id
        , entity_type
        , target_id 
        , target_type
        , context 
        , COUNT(DISTINCT id) AS session_count
        , COUNT(DISTINCT completed_id) AS successful_session_count
        , COUNT(DISTINCT no_data_found_id) AS no_data_session_count
        , MIN(created_at) AS first_session_on
        , MAX(created_at) AS last_session_on
        , MIN(completed_created_at) AS first_successful_session_on
        , MAX(completed_created_at) AS last_successful_session_on
    FROM sessions
    GROUP BY entity_id, entity_type, target_id, target_type, context
    """
).createOrReplaceTempView("facts")

spark.sql("SELECT * FROM facts").show(1000, False)


