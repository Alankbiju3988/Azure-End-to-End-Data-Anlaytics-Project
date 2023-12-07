# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://olist-raw-data@olistdata.blob.core.windows.net/",
    mount_point = "/mnt/olist",
    extra_configs = {"fs.azure.account.key.olistdata.blob.core.windows.net": "<ACCOUNT KEY>"}
)


# COMMAND ----------

base_path="/mnt/olist/raw-data/olist_db"

# COMMAND ----------

from pyspark.sql import SparkSession
import os

# Create a Spark session
spark = SparkSession.builder.appName("CSVLoader").getOrCreate()

# Specify the base directory where your folders are located
base_directory = "/mnt/olist/raw-data/olist_db/"  

# List the directories in the base directory
directories = [item.path for item in dbutils.fs.ls(base_directory) if item.isDir()]

# Loop through the directories and load each CSV into a DataFrame
dataframes = {}

for directory in directories:
    folder_name = directory.split("/")[-2]
    csv_file_path = f"{directory}/{folder_name}.csv"
    # Print the folder name
    print(f"Processing folder: {folder_name}")

    # Load CSV into DataFrame
    try:
        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
        # Store the DataFrame in a dictionary with folder name as the key
        dataframes[folder_name] = df
    except Exception as e:
        print(f"Error loading data from {csv_file_path}: {str(e)}")


# COMMAND ----------

display(dataframes["olist_customers_dataset"])

# COMMAND ----------

# Loop through the dictionary and print the data types for each DataFrame
for folder_name, df in dataframes.items():
    display(df)
    print(f"Data types for DataFrame in folder {folder_name}:")
    for column_name, data_type in df.dtypes:
        print(f"Column: {column_name}, Data Type: {data_type}")
    print("\n")  # Add a newline for better readability


# COMMAND ----------

# MAGIC %md
# MAGIC - review_creation_date, Data Type: string in olist_order_reviews_dataset
# MAGIC

# COMMAND ----------

# to change the review_creation_date, Data Type: string in olist_order_reviews_dataset into date format
from pyspark.sql.functions import to_date
from pyspark.sql.types import TimestampType

# Assuming your DataFrame is named df
df = dataframes["olist_order_reviews_dataset"]

# Specify the column name
column_name1 = "review_creation_date"
# Convert date strings to timestamp type
df = df.withColumn(column_name1, to_date(df[column_name1], 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))
# Store the modified DataFrame back into the dictionary
dataframes["olist_order_reviews_dataset"] = df

# Optionally, show the updated DataFrame
df.show(truncate=False)

# COMMAND ----------

display(dataframes["olist_order_reviews_dataset"])
display(dataframes["olist_order_reviews_dataset"].dtypes)

# COMMAND ----------

# To save this into cleaned data in parquet format
# Define the path for the cleaned data
cleaned_data_path = "/mnt/olist/cleaned_data"

# Loop through the dictionary and save each DataFrame to Parquet
for folder_name, df in dataframes.items():
    print(f"Saving DataFrame in folder {folder_name}:")

    # Save the DataFrame to Parquet
    df.write.parquet(f"{cleaned_data_path}/{folder_name}", mode="overwrite")

    print(f"DataFrame saved in {cleaned_data_path}/{folder_name}\n")

# COMMAND ----------


