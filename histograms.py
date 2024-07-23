import os
import sys
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

# Read the Parquet file
parquet_file_path = "data/yellow_tripdata_2024-01.parquet"
df = spark.read.parquet(parquet_file_path)

# Select the required columns
selected_columns = ['fare_amount', 'passenger_count']
new_df = df.select(selected_columns)

cleaned_df = new_df.dropna()

# Convert to RDDs
fare_rdd = cleaned_df.select('fare_amount').rdd.flatMap(lambda x: x)
passenger_count_rdd = cleaned_df.select('passenger_count').rdd.flatMap(lambda x: x)

# Collect data from RDDs
fare_data = fare_rdd.collect()
passenger_count_data = passenger_count_rdd.collect()

# Define bin edges
fare_bins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 50, 100]
passenger_count_bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Plot histograms using Matplotlib
plt.figure(figsize=(12, 6))

# Fare amount histogram
plt.subplot(1, 2, 1)
plt.hist(fare_data, bins=fare_bins, edgecolor='black')
plt.title('Fare Amount Histogram')
plt.xlabel('Fare Amount')
plt.ylabel('Frequency')

# Passenger count histogram
plt.subplot(1, 2, 2)
plt.hist(passenger_count_data, bins=passenger_count_bins, edgecolor='black')
plt.title('Passenger Count Histogram')
plt.xlabel('Passenger Count')
plt.ylabel('Frequency')

# Show the plots
plt.tight_layout()
plt.show()

# Stop the Spark session
spark.stop()
