from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round


# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

# Read the Parquet file
parquet_file_path = "data/yellow_tripdata_2024-01.parquet"
df = spark.read.parquet(parquet_file_path)


# Select the required columns
selected_columns = ['VendorID',  'fare_amount', 'tip_amount', 'tpep_pickup_datetime',
                     'tpep_dropoff_datetime', 'trip_distance']  
new_df = df.select(selected_columns)


#remove rows with empty feilds
cleaned_df = new_df.dropna()

# Calculate trip duration in hours
cleaned_df = cleaned_df.withColumn(
    "trip_duration_hours",
    round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600, 2)  # 3600 seconds in an hour
)

# Calculate tip percentage
cleaned_df = cleaned_df.withColumn(
    "tip_percentage",
    round((col("tip_amount") / col("fare_amount")) * 100, 2)
)

# Calculate trip speed (distance per duration)
cleaned_df = cleaned_df.withColumn(
    "trip_speed",
    round(col("trip_distance") / col("trip_duration_hours"), 2)
)
# Show the results
cleaned_df.show()

# Optionally, print the schema of the new DataFrame
cleaned_df.printSchema()

# Stop the Spark session
spark.stop()