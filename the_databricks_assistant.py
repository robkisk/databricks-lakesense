# Databricks notebook source
# MAGIC %md
# MAGIC ## The Databricks Assistant
# MAGIC
# MAGIC The Databricks Assistant works as an AI-based companion pair-programmer to make you more efficient as you create notebooks, queries, and files. It can help you rapidly answer questions by **generating, optimizing, completing, explaining, and fixing code and queries.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Code

# COMMAND ----------

import pandas as pd

# Change this to point to a hive or Unity Catalog table in your workspace
# Read the sample NYC Taxi Trips dataset and load it into a DataFrame
df = spark.read.table("samples.nyctaxi.trips")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyze Data
# MAGIC
# MAGIC Prompt:
# MAGIC
# MAGIC _generate pandas code to convert the pyspark dataframe to a pandas dataframe and select the 10 most expensive trips from df based on the fare_amount column and display the dataframe_

# COMMAND ----------

import pandas as pd

df_pd = df.select("*").toPandas()
expensive_trips = df_pd.nlargest(10, "fare_amount")

expensive_trips

# COMMAND ----------

import pandas as pd

# Select the 10 most expensive trips based on the `fare_amount` column
most_expensive_trips = df.orderBy("fare_amount", ascending=False).limit(10)

# Convert the PySpark DataFrame to a Pandas DataFrame
most_expensive_trips_pd = most_expensive_trips.toPandas()

# COMMAND ----------

display(most_expensive_trips)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a DataFrame reader
# MAGIC

# COMMAND ----------

# example data inside dbfs. Switch to any dataset in your workspace
display(dbutils.fs.ls("dbfs:/databricks-datasets/bikeSharing/data-001/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Prompt:
# MAGIC
# MAGIC _Generate code to read the day.csv file in the bikeSharing dataset_
# MAGIC

# COMMAND ----------

# HIDE THIS CELL FOR DEMO

dayDf = (
    spark.read.format("csv")
    .option("header", "true")
    .load("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform & Optimize Code

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Pandas to PySpark
# MAGIC
# MAGIC Prompt:
# MAGIC
# MAGIC 1. _Convert this code to PySpark_
# MAGIC
# MAGIC 2. _Convert this code in Cmd 15 to PySpark and explain the code to me step by step_

# COMMAND ----------

# MAGIC %md
# MAGIC #### Improve code efficiency
# MAGIC
# MAGIC Prompt:
# MAGIC
# MAGIC _Write me a function to benchmark the execution of code in cell 15, then give me another way to write this code that is more efficient and would perform better in the benchmark_
# MAGIC
# MAGIC (An alternative prompt) _Show me a code example of inefficient python code, explain why it is inefficient, and then show me an improved version of that code that is more efficient. Explain why it is more efficient, then give me a list of strings to test this out with and the code to benchmark trying each one out._

# COMMAND ----------

import pandas as pd

# Load the diamonds.csv dataset into a pandas DataFrame
data = pd.read_csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

# Filter the dataset to only show diamonds with carat greater than 3 and a price less than 10000
filtered_data = data[(data["carat"] > 3) & (data["price"] < 10000)]

# Group the filtered dataset by cut and calculate the average price and carat for each cut
grouped_data = filtered_data.groupby("cut").agg({"price": "mean", "carat": "mean"})

# Show the result
grouped_data

# COMMAND ----------

# Load the diamonds.csv dataset into a PySpark DataFrame
data = spark.read.csv(
    "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",
    header=True,
    inferSchema=True,
)

# Filter the dataset to only show diamonds with carat greater than 3 and a price less than 10000
filtered_data = data.filter((data["carat"] > 3) & (data["price"] < 10000))

# Group the filtered dataset by cut and calculate the average price and carat for each cut
from pyspark.sql.functions import avg

grouped_data = filtered_data.groupBy("cut").agg(
    avg("price").alias("avg_price"), avg("carat").alias("avg_carat")
)

# Show the result
display(grouped_data)

# COMMAND ----------

from pyspark.sql.functions import avg

# Load the diamonds.csv dataset into a PySpark DataFrame
data = spark.read.csv(
    "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",
    header=True,
    inferSchema=True,
)

# Filter the dataset to only show diamonds with carat greater than 3 and a price less than 10000
filtered_data = data.filter((data["carat"] > 3) & (data["price"] < 10000))

# Group the filtered dataset by cut and calculate the average price and carat for each cut
grouped_data = filtered_data.groupBy("cut").agg(
    avg("price").alias("avg_price"), avg("carat").alias("avg_carat")
)

# Show the result
grouped_data.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Code

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reverse a string
# MAGIC
# MAGIC By pressing **control + shift + space** (on MacOS) directly in a cell, LakeSense will use comments as context to generate code. _(place cursor on line below comment)_
# MAGIC
# MAGIC Press **tab** to autocomplete the suggestion.

# COMMAND ----------

# Write code to reverse a string.  Just the code; no explanatory text.
string[::-1]

# COMMAND ----------

# Write code to reverse a string.  Just the code; no explanatory text.
string[::-1]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform EDA
# MAGIC
# MAGIC Prompt:
# MAGIC
# MAGIC _Load the wine dataset into a Dataframe from sklearn, bucket the data into 3 groups by quality, then visualize in a plotly barchart_
# MAGIC

# COMMAND ----------

import pandas as pd
import plotly.express as px
from sklearn.datasets import load_wine

# load the wine dataset into a DataFrame (from sklearn package)
wine = load_wine()
df = pd.DataFrame(data=wine.data, columns=wine.feature_names)
df["quality"] = wine.target

# bin the quality values into 3 groups
df["quality_group"] = pd.cut(
    df["quality"], bins=[0, 4, 6, 9], labels=["low", "medium", "high"]
)

# create a bar chart with Plotly
fig = px.histogram(df, x="quality_group")

# show the chart
fig.show()

# COMMAND ----------

import pandas as pd
import plotly.express as px
from sklearn.datasets import load_wine

# Load the wine dataset from scikit-learn
data = load_wine()

# Create a Pandas DataFrame from the data
df = pd.DataFrame(data.data, columns=data.feature_names)
df["quality"] = data.target

# Create three labeled buckets for quality
df["quality_group"] = pd.cut(
    df["quality"], bins=[0, 4, 6, 9], labels=["low", "medium", "high"]
)

# Count the number of wines in each quality group
grouped_df = df["quality_group"].value_counts().reset_index()

# Create a Plotly bar chart visualizing the counts of wines in each quality group
fig = px.bar(grouped_df, x="index", y="quality_group")
fig.show()

# COMMAND ----------

# HIDE THIS CELL BEFORE DEMO

import pandas as pd
import plotly.graph_objs as go
from sklearn.datasets import load_wine

# Load the wine dataset from scikit-learn
data = load_wine()

# Create a Pandas DataFrame from the data
df = pd.DataFrame(data.data, columns=data.feature_names)
df["quality"] = data.target


# Define a function to bin the quality column into three groups
def bucket_quality(quality):
    if quality < 5:
        return "Low"
    elif quality > 6:
        return "High"
    else:
        return "Medium"


# Apply the function to the quality column to create a new column 'quality_bucket'
df["quality_bucket"] = df["quality"].apply(bucket_quality)

# Count the number of wines in each quality bucket and store the result in grouped_df DataFrame
grouped_df = df.groupby("quality_bucket").count().reset_index()

# Create a Plotly bar chart visualizing the counts of wines in each quality bucket
fig = go.Figure(data=[go.Bar(x=grouped_df["quality_bucket"], y=grouped_df["quality"])])
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explain Code

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic code explanation
# MAGIC
# MAGIC Prompt:
# MAGIC
# MAGIC 1. _Explain what this code does_
# MAGIC 2. _Add comments to this code to explain what it does_
# MAGIC

# COMMAND ----------

# Read the sample NYC Taxi Trips dataset and load it into a DataFrame
df = spark.read.table("samples.nyctaxi.trips")

# COMMAND ----------

import pandas as pd
import plotly.express as px
from sklearn.datasets import load_wine

# load the wine dataset into a DataFrame
wine = load_wine()
df = pd.DataFrame(data=wine.data, columns=wine.feature_names)
df["quality"] = wine.target

# bin the quality values into 3 groups
df["quality_group"] = pd.cut(
    df["quality"], bins=[0, 4, 6, 9], labels=["low", "medium", "high"]
)

# create a bar chart with Plotly
fig = px.histogram(df, x="quality_group")

# show the chart
fig.show()

# COMMAND ----------

import pyspark.sql.functions as F

fare_by_route = (
    df.groupBy("pickup_zip", "dropoff_zip")
    .agg(
        F.sum("fare_amount").alias("total_fare"),
        F.count("fare_amount").alias("num_trips"),
    )
    .sort(F.col("num_trips").desc())
)

display(fare_by_route)


# COMMAND ----------

# Import the PySpark SQL functions module
import pyspark.sql.functions as F

# Group the DataFrame by pickup and dropoff zip codes, aggregate by summing fare_amount and counting the number of rows
# Set alias names for sum of fare_amount and count of fare_amount
# Sort the result by the number of trips in descending order
fare_by_route = (
    df.groupBy("pickup_zip", "dropoff_zip")
    .agg(
        F.sum("fare_amount").alias("total_fare"),
        F.count("fare_amount").alias("num_trips"),
    )
    .sort(F.col("num_trips").desc())
)

# Display the resulting DataFrame in a table
display(fare_by_route)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fast documentation lookups
# MAGIC
# MAGIC Prompt:
# MAGIC
# MAGIC 1. _When should I use repartition() vs. coalesce() in Apache Spark?_
# MAGIC
# MAGIC 2. _What is the difference between the various pandas_udf functions (in PySpark and Pandas on Spark/Koalas) and when should I choose each?  Can you show me an example of each with the diamonds dataset?_
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ask How-to questions
# MAGIC
# MAGIC Prompt:
# MAGIC
# MAGIC 1. _Tell me how to find out which jobs are running the longest_
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix Code

# COMMAND ----------

# MAGIC %md
# MAGIC #### Code Debugging
# MAGIC
# MAGIC The following cell will error due to a missing import statement.  Run the cell to trigger the error, then use the prompt:
# MAGIC
# MAGIC _How do I fix this error?  What is 'S'?_

# COMMAND ----------

fare_by_route = (
    df.groupBy("pickup_zip", "dropoff_zip")
    .agg(
        S.sum("fare_amount").alias("total_fare"),
        S.count("fare_amount").alias("num_trips"),
    )
    .sort(S.col("num_trips").desc())
)

display(fare_by_route)

# COMMAND ----------

from pyspark.sql.functions import count as C
from pyspark.sql.functions import sum as S

# COMMAND ----------


fare_by_route = (
    df.groupBy("pickup_zip", "dropoff_zip")
    .agg(S("fare_amount").alias("total_fare"), C("fare_amount").alias("num_trips"))
    .sort(S("num_trips").desc())
)

display(fare_by_route)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Error debugging
# MAGIC
# MAGIC The following code will throw a fairly simple `“AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]”` error.

# COMMAND ----------

import pandas as pd

df_pd = df.select("*").toPandas()
expensive_trips = df_pd.nlargest(10, "fare_amount")

expensive_trips

# COMMAND ----------

from pyspark.sql.functions import col

# create a dataframe with two columns: a and b
df = spark.range(5).select(col("id").alias("a"), col("id").alias("b"))

# add a new column c
df = df.withColumn("c", col("a") + col("b"))

# now you can select column c
df.select(col("c")).show()

# COMMAND ----------

from pyspark.sql.functions import col

# create a dataframe with two columns: a and b
df = spark.range(5).select(col("id").alias("a"), col("id").alias("b"))

# add a new column named c
df = df.withColumn("c", col("a") + col("b"))

# select column 'c'
df.select(col("c")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Prompt:
# MAGIC
# MAGIC Why am I getting this error and how do I fix it?
