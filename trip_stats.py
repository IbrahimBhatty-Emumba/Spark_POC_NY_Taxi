from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, unix_timestamp, desc

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

# Read the Parquet file
parquet_file_path = "data/yellow_tripdata_2024-01.parquet"
df = spark.read.parquet(parquet_file_path)


# Select the required columns
selected_columns = ['VendorID', 'tpep_pickup_datetime',
                     'tpep_dropoff_datetime', 'passenger_count', 
                     'trip_distance', 'fare_amount', 'PULocationID', 'DOLocationID']  
new_df = df.select(selected_columns)


#remove rows with empty feilds
cleaned_df = new_df.dropna()

# Calculate trip duration in minutes
df = df.withColumn("trip_duration", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)

# Find the top 10 most frequent PULocationID
top_pu_locations = df.groupBy("PULocationID") \
                     .count() \
                     .orderBy(desc("count")) \
                     .limit(10) \
                     .collect()

# Find the top 10 most frequent DOLocationID
top_do_locations = df.groupBy("DOLocationID") \
                     .count() \
                     .orderBy(desc("count")) \
                     .limit(10) \
                     .collect()

#printing the popuar picup and drop of locations
print("Top 10 PULocationID:")
for row in top_pu_locations:
    print(f"PULocationID: {row['PULocationID']}, Count: {row['count']}")

print("\nTop 10 DOLocationID:")
for row in top_do_locations:
    print(f"DOLocationID: {row['DOLocationID']}, Count: {row['count']}")


trip_stats = cleaned_df.agg(
    _sum('trip_distance').alias('total_trip_distance'),
    avg('trip_distance').alias('avg_trip_distance')
).collect()[0]

# Extract the total and average trip distance
total_trip_distance = trip_stats['total_trip_distance']
avg_trip_distance = trip_stats['avg_trip_distance']
avg_trip_duration = df.agg(avg("trip_duration")).first()[0]


# Print the trip stats
print(f"Total sum of trip_distance: {total_trip_distance}")
print(f"Average trip_distance: {avg_trip_distance}")
print(f"Average Trip Duration: {avg_trip_duration} minutes")


# Show the contents of the new DataFrame
cleaned_df.show()


# Optionally, print the schema of the new DataFrame
cleaned_df.printSchema()

# Stop the Spark session
spark.stop()