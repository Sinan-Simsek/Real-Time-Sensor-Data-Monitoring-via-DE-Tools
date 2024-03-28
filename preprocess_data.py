import os
from functools import reduce
import findspark

findspark.init("/opt/spark")  # This is where local spark is installed
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

# Initialize SparkSession
spark_session = SparkSession.builder \
    .appName("Spark Read Write") \
    .master("local[2]") \
    .getOrCreate()

# Read files and create separate dataframes
room_data = {}
directory_path = '/home/train/final_project/KETI'
dataframes_per_room = {}
sensor_columns = ['co2', 'humidity', 'light', 'pir', 'temperature']

# Create seperate dataframes for each room
for folder_name in os.listdir(directory_path): # loop through the folders under KETI
    folder_path = directory_path + '/' + folder_name # e.g. /home/train/final_project/KETI/419
    count = 0
    for file_name in os.listdir(folder_path):  # loop through the files under each room folder
        file_path = os.path.join(folder_path, file_name)   # e.g. /home/train/final_project/KETI/419/co2.csv
        df_key = f"{folder_name}_{file_name.split('.')[0]}"  # e.g. 419_co2
        room_data[df_key] = spark_session.read.csv(file_path)
        room_data[df_key] = room_data[df_key].toDF('ts_min_bignt', sensor_columns[count])  # Sample key: 419_co2. Dataframe columns: ts_min_bignt, co2
        count += 1

    # Create temporary views for each dataframe for Spark SQL part
    for df_name, df in room_data.items():
        df.createOrReplaceTempView(df_name)

    # SQL query to join dataframes on ts_min_bignt and create dataframe per room
    query = f"""
    SELECT
      df_co2.*,
      df_humidity.humidity,
      df_light.light,
      df_pir.pir,
      df_temperature.temperature        
    FROM
      df_co2
      INNER JOIN df_humidity ON df_co2.ts_min_bignt = df_humidity.ts_min_bignt
      INNER JOIN df_light ON df_humidity.ts_min_bignt = df_light.ts_min_bignt
      INNER JOIN df_pir ON df_light.ts_min_bignt = df_pir.ts_min_bignt
      INNER JOIN df_temperature ON df_pir.ts_min_bignt = df_temperature.ts_min_bignt
    """
    dataframes_per_room[folder_name] = spark_session.sql(query).withColumn("room", F.lit(folder_name))

# Merge all room dataframes and sort based on timestamp values
dfs_to_concatenate = []
for i in dataframes_per_room.values():
    dfs_to_concatenate.append(i)

df_merged = reduce(DataFrame.unionAll, dfs_to_concatenate).sort(F.col("ts_min_bignt")).dropna()

# Create main dataframe
df_main = df_merged.withColumn("event_ts_min", F.from_unixtime(F.col("ts_min_bignt"), "yyyy-MM-dd HH:mm:ss"))

# Write main dataframe to a CSV file
df_main.toPandas().to_csv("/home/train/data-generator/input/sensors.csv", index=False)
