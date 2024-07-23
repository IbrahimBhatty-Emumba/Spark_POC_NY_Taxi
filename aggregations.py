from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, unix_timestamp, desc, to_date, weekofyear, month, hour, dayofweek

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Aggregate Trip Data") \
    .getOrCreate()

# Read the Parquet file
parquet_file_path = "data/yellow_tripdata_2024-01.parquet"
df = spark.read.parquet(parquet_file_path)

# Select the required columns
selected_columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'fare_amount', 'trip_distance']
new_df = df.select(selected_columns)

# Remove rows with empty fields
cleaned_df = new_df.dropna()

# Calculate trip duration in minutes
cleaned_df = cleaned_df.withColumn("trip_duration", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)

# Aggregate daily
daily_agg = cleaned_df.withColumn("date", to_date("tpep_pickup_datetime")) \
                      .groupBy("date") \
                      .agg(
                          _sum('trip_distance').alias('total_trip_distance'),
                          avg('trip_distance').alias('avg_trip_distance'),
                          avg('trip_duration').alias('avg_trip_duration'),
                          avg('fare_amount').alias('avg_fare_amount')
                      ).orderBy("date")
                      

# Aggregate weekly
weekly_agg = cleaned_df.withColumn("week", weekofyear("tpep_pickup_datetime")) \
                       .groupBy("week") \
                       .agg(
                           _sum('trip_distance').alias('total_trip_distance'),
                           avg('trip_distance').alias('avg_trip_distance'),
                           avg('trip_duration').alias('avg_trip_duration'),
                           avg('fare_amount').alias('avg_fare_amount')
                       )

# Aggregate monthly
monthly_agg = cleaned_df.withColumn("month", month("tpep_pickup_datetime")) \
                        .groupBy("month") \
                        .agg(
                            _sum('trip_distance').alias('total_trip_distance'),
                            avg('trip_distance').alias('avg_trip_distance'),
                            avg('trip_duration').alias('avg_trip_duration'),
                            avg('fare_amount').alias('avg_fare_amount')
                        )

# Average fare by hour of the day
hourly_fare_agg = cleaned_df.withColumn("hour", hour("tpep_pickup_datetime") + 1) \
                            .groupBy("hour") \
                            .agg(
                                avg('fare_amount').alias('avg_fare_amount')
                            ) \
                            .orderBy("hour")

# Average fare by day of the week
daily_of_week_fare_agg = cleaned_df.withColumn("day_of_week", dayofweek("tpep_pickup_datetime")) \
                                    .groupBy("day_of_week") \
                                    .agg(
                                        avg('fare_amount').alias('avg_fare_amount')
                                    ) \
                                    .orderBy("day_of_week")

# Show results
print("Daily Aggregation:")
daily_agg.show()

print("Weekly Aggregation:")
weekly_agg.show()

print("Monthly Aggregation:")
monthly_agg.show()

print("Average Fare by Hour of the Day:")
hourly_fare_agg.show()

print("Average Fare by Day of the Week:")
daily_of_week_fare_agg.show()

# Stop the Spark session
spark.stop()
