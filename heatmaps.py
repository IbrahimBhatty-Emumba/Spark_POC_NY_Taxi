from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

# Read the Parquet file
parquet_file_path = "data/yellow_tripdata_2024-01.parquet"
df = spark.read.parquet(parquet_file_path)

# Group by PULocationID and DOLocationID to count the occurrences
pickup_counts = df.groupBy("PULocationID").agg(count("*").alias("count")).orderBy("PULocationID")
dropoff_counts = df.groupBy("DOLocationID").agg(count("*").alias("count")).orderBy("DOLocationID")

pickup_pandas_df = pickup_counts.toPandas()
dropoff_pandas_df = dropoff_counts.toPandas()

# Print the data types to verify conversion to integers
print(pickup_pandas_df.dtypes)
print(dropoff_pandas_df.dtypes)

# Create pivot tables
pickup_pivot = pickup_pandas_df.pivot_table(index="PULocationID", values="count", aggfunc='sum')
dropoff_pivot = dropoff_pandas_df.pivot_table(index="DOLocationID", values="count", aggfunc='sum')

# Print a sample of the data to verify contents
print(pickup_pivot.head())
print(dropoff_pivot.head())

# Find the range of IDs
all_pu_ids = pd.Index(range(pickup_pandas_df['PULocationID'].min(), pickup_pandas_df['PULocationID'].max() + 1))
all_do_ids = pd.Index(range(dropoff_pandas_df['DOLocationID'].min(), dropoff_pandas_df['DOLocationID'].max() + 1))

# Print the range of IDs
print("Range of PULocationID:", all_pu_ids.min(), "to", all_pu_ids.max())
print("Range of DOLocationID:", all_do_ids.min(), "to", all_do_ids.max())
# Set up the matplotlib figure
plt.figure(figsize=(15, 12))

# Draw the heatmap for pickup locations
plt.subplot(2, 1, 1)
sns.heatmap(pickup_pivot, cmap="Reds", annot=False, fmt="g", cbar=False)
plt.title("Heatmap of Pickup Locations")
plt.xlabel("Count")
plt.ylabel("PULocationID")

# Draw the heatmap for dropoff locations
plt.subplot(2, 1, 2)
sns.heatmap(dropoff_pivot, cmap="Reds", annot=False, fmt="g", cbar=False)
plt.title("Heatmap of Dropoff Locations")
plt.xlabel("Count")
plt.ylabel("DOLocationID")

# Adjust the layout
plt.tight_layout()

# Show the plot
plt.show()
