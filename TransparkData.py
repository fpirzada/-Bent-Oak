from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, when
from dotenv import load_dotenv
import os

load_dotenv()


# Get database connection parameters from environment variables
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
schema_name = os.getenv("SCHEMA_NAME")
spark_memory_config = "8g"  # Adjust based on your available memory
driver_jar_path ='postgresql-42.7.1.jar'# Create a Spark session
spark = SparkSession.builder \
            .appName("DishwasherVolumeMetrics") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.driver.memory", spark_memory_config) \
            .config("spark.executor.memory", spark_memory_config) \
            .config("spark.executor.instances", "4")  \
            .config("spark.executor.cores", "4")  \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.jars",driver_jar_path) \
            .config('spark.driver.extraClassPath', driver_jar_path) \
            .config("spark.driver.extraClassPath", driver_jar_path) \
            .getOrCreate()

# Define PostgreSQL connection properties
postgres_properties = {
    "driver": "org.postgresql.Driver",
    "url": f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
    "user": db_username,
    "password": db_password,
    "schema": schema_name  # Optional: Set the schema if needed
}

# Load the data into DataFrames
rb_dishwashweekly_gb_fct = (
    spark.read
    .jdbc(postgres_properties["url"], "rb_dishwashweekly_gb_fct_07072021", properties=postgres_properties)
)

rb_dishwashweekly_gb_per = (
    spark.read
    .jdbc(postgres_properties["url"], "rb_dishwashweekly_gb_per_07072021", properties=postgres_properties)
)

rb_dishwashweekly_gb_prod = (
    spark.read
    .jdbc(postgres_properties["url"], "rb_dishwashweekly_gb_prod_07072021", properties=postgres_properties)
)

rb_dishwashweekly_gb_fact_data = (
    spark.read
    .jdbc(postgres_properties["url"], "rb_dishwashweekly_gb_fact_data_07072021", properties=postgres_properties)
)
rb_dishwashweekly_gb_mkt = (
    spark.read
    .jdbc(postgres_properties["url"], "rb_dishwashweekly_gb_mkt_07072021", properties=postgres_properties)
)


# Assuming that the TAG column is a common key across tables, you can join them
# For simplicity, let's assume we are working with a specific period in this example
period_condition = (rb_dishwashweekly_gb_fct["TAG"] == "W2018027")

# Join tables based on the period condition
joined_data = (
    rb_dishwashweekly_gb_fct.join(rb_dishwashweekly_gb_per, "TAG", "inner")
    .join(rb_dishwashweekly_gb_prod, "TAG", "inner")
    .join(rb_dishwashweekly_gb_fact_data, "TAG", "inner")
    .join(rb_dishwashweekly_gb_mkt, "TAG", "inner")
)
# Explicitly qualify the column references with table aliases
volume_metrics = (
    joined_data
    .withColumn("Volume_Metric", when(col("rb_dishwashweekly_gb_fct.SHORT") == "Volume", col("rb_dishwashweekly_gb_fct.F000000000000000000200000000000000000000")))
    .groupBy("rb_dishwashweekly_gb_fct.PROD_TAG")
    .agg(sum("Volume_Metric").alias("Total_Volume"))
)

# Show the results
print("Volume Metrics:")
volume_metrics.show()
# Stop the Spark session
spark.stop()