from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, countDistinct, max_by, when, max as pmax, min as pmin, min_by

spark = SparkSession.builder.config("spark.driver.memory", "8g").appName('SparkByExamples.com').getOrCreate()

scrap_sess_df = spark.read.parquet("data/*.parquet")

successful_id = when(col("status") == "COMPLETED", col("id")).otherwise(None)
successful_created_at = when(col("status") == "COMPLETED", col("created_at")).otherwise(None)
scrap_sess_df = scrap_sess_df.withColumn("successful_id", successful_id).withColumn(
        "successful_created_at", successful_created_at)

target_type = when(col("terminal_id").isNotNull(), "terminal").otherwise("shipping_line")
scrap_sess_df = scrap_sess_df.withColumn("target_type", target_type)
scrap_sess_df = scrap_sess_df.withColumn("target_id",
                            coalesce(col("terminal_id"), col("shipping_line_id"))).drop(
                                "terminal_id", "shipping_line_id")

entity_type = when(col("transaction_id").isNotNull(), "transaction").otherwise("shipment")
scrap_sess_df = scrap_sess_df.withColumn("entity_type", entity_type)
scrap_sess_df = scrap_sess_df.withColumn("entity_id", 
                             coalesce("transaction_id", "shipment_id")).drop(
                                 "transaction_id", "shipment_id")

# Grouping! 
stats = scrap_sess_df.groupBy("entity_id", "entity_type", "target_id", "target_type", "context").agg(
    countDistinct("id").alias("scraping_session_count"),
    countDistinct("successful_id").alias("successful_scraping_session_count"),
    # countDistinct("target_id").alias("distinct_target_count"),
    pmin("created_at").alias("first_session_on"),
    # min_by("target_id", "created_at").alias("first_session_target"),
    pmin("successful_created_at").alias("first_successful_session_on"),
    # min_by("target_id", "successful_created_at").alias("first_successful_session_target"),
    pmax("created_at").alias("last_session_on"),
    # max_by("target_id", "created_at").alias("last_session_target"),
    pmax("successful_created_at").alias("last_successful_session_on"),
    # max_by("target_id", "successful_created_at").alias("last_successful_session_target"),
)
# print(stats.count())
stats.write.parquet("scrap_sess_facts")
