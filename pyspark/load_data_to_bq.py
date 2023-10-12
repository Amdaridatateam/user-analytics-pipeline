from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder.appName("MovieAnalytics").getOrCreate()

staging_bucket = "useranalytics-pipeline-bucket-staging"
processed_log_reviews = f'gs://{staging_bucket}/log_review/'
processed_movie_reviews = f'gs://{staging_bucket}/movie_review/'
GCP_PROJECT_ID = 'user-behaviour-project'
DATASET_ID = 'user_analytics'

log_revi
log_review_df = spark.read.parquet(processed_log_reviews, inferSchema=True)
movie_review_df = spark.read.parquet(processed_movie_reviews, inferSchema=True)

data = movie_review_df \
    .join(log_review_df, movie_review_df.log_id == log_review_df.review_id, "left") \
    .join(user_purchase_df, movie_review_df.user_id == user_purchase_df.customer_id, "left") \
    .select(
        col("log_date").cast("date"),
        col("location"),
        col("device"),
        col("os"),
        col("positive_review"),
        col("review_id").alias("review_count"),
    )

os_browser_mapping = {'Google Android': "Google Chrome", 'Apple iOS': 'Safari', 'Linux': 'Firefox', 'Apple MacOS': 'Safari', 'Microsoft Windows': 'Edge'}

data = data.withColumn("browser", when(col("os").isin(os_browser_mapping.keys()), col("os")).otherwise(None))
data = data.withColumn("browser", data["browser"].cast("string"))

data.show()

dim_date = data.selectExpr(
    "log_date AS date", 
    "day(log_date) AS day", 
    "month(log_date) AS month", 
    "year(log_date) AS year", 
    "CASE \
        WHEN month(log_date) IN (12, 1, 2) THEN 'Winter' \
        WHEN month(log_date) IN (3, 4, 5) THEN 'Spring' \
        WHEN month(log_date) IN (6, 7, 8) THEN 'Summer' \
        ELSE 'Fall' END AS season"
    ).distinct().withColumn("id_date", monotonically_increasing_id())

dim_date.show()

dim_device = data.select("device").distinct().withColumn("id_device", monotonically_increasing_id())
dim_device.show()

dim_location = data.select("location").distinct().withColumn("id_location", monotonically_increasing_id())
dim_location.show()

dim_browser = data.select("browser").distinct().withColumn("id_browser", monotonically_increasing_id())
dim_browser.show()

dim_os = data.select("os").distinct().withColumn("id_os", monotonically_increasing_id())
dim_os.show()

fact_movie_analytics = data.join(dim_device, data.device == dim_device.device, "inner") \
    .join(dim_location, data.location == dim_location.location, "inner") \
    .join(dim_os, data.os == dim_os.os, "inner") \
    .join(dim_browser, data.browser == dim_os.browser, "inner") \
    .join(dim_date, data.date == dim_date.date, "inner") \
    .select(
        "dim_device.id_device AS id_device", 
        "dim_location.id_location AS id_location", 
        "dim_os.id_os AS id_os", 
        "dim_browser.id_browser AS id_browser", 
        "dim_date.id_date AS id_log_date", 
        "positive_review AS review_score", 
        "review_count"
    ).withColumn("id_movie_analytics", monotonically_increasing_id())


fact_movie_analytics.show()

fact_movie_analytics.write \
    .format("bigquery") \
    .option("table", f"{GCP_PROJECT_ID}.{DATASET_ID}.fact_movie_analytics") \
    .mode("overwrite") \
    .save()

dim_date.write \
    .format("bigquery") \
    .option("table", f"{GCP_PROJECT_ID}.{DATASET_ID}.dim_date") \
    .mode("overwrite") \
    .save()

dim_device.write \
    .format("bigquery") \
    .option("table", f"{GCP_PROJECT_ID}.{DATASET_ID}.dim_device") \
    .mode("overwrite") \
    .save()

dim_location.write \
    .format("bigquery") \
    .option("table", f"{GCP_PROJECT_ID}.{DATASET_ID}.dim_location") \
    .mode("overwrite") \
    .save()

dim_os.write \
    .format("bigquery") \
    .option("table", f"{GCP_PROJECT_ID}.{DATASET_ID}.dim_os") \
    .mode("overwrite") \
    .save()

dim_browser.write \
    .format("bigquery") \
    .option("table", f"{GCP_PROJECT_ID}.{DATASET_ID}.dim_browser") \
    .mode("overwrite") \
    .save()

# Stop the SparkSession
spark.stop()
