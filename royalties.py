from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from google.cloud import storage
from google.cloud import firestore
import requests
import json
import csv
import os 
from datetime import datetime
"""
Generates using Spark a dialy report 
of the top 10 best-selling resources in each category

Resources info in FireStore
Events info in Storage

In case there are events from several days
one report by day will be generated

Reports are stored in csv files in Google Cloud Storage
with this schema:
position|date|categoryId|categoryName|resourceId|resourceName

"""

# ------------------------------ SOME DATA ------------------------------------
exchange_rates_url = "https://bigdata.luisbelloch.es/exchange_rates/usd"
categories_url = "https://singularity.luisbelloch.es/v1/stripe/categories"
countries_url = "https://bigdata.luisbelloch.es/countries.csv"
reports_bucket_name = "royalties-resources-month-report-big-3"
events_bucket_name = "events-stripe-big-3"
project_id="Stripe-big-3"


# --------------------------- SOME FUNCTIONS ----------------------------------

def get_categories(spark, url):
    """Get categories content from URL

    Args:
        spark (Spark session): Spark session
        url (str): URL to get the information

    Returns:
        Spark DataFrame: Categories content from URL
    """
       
    response = requests.get(url)
    data = response.json()
    content = data["content"]
    
    categories_schema = StructType([
        StructField("tenant", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("percent", StringType(), True)
    ])
    
    df = spark.createDataFrame(content, schema = categories_schema)
    
    return df


def get_countries(spark, url):
    df = spark.read.csv(url, header=True, inferSchema=True)    
    return df


def clean_categories(df):
    """Select columns of interest for the report
    rename columns and normalize categoryId
    in categories spark dataframe

    Args:
        df (Spark Dataframe): categories obtained from URL

    Returns:
        Spark Dataframe: categories ready to join
    """
    
    # Select columns of interest and rename them
    df = df.select(F.col("id").alias("categoryId"), 
                   F.col("percent"))
    
    #Normalize categoryId adding "0" after the dot
    df = df.withColumn("categoryId", 
                       F.regexp_replace(F.col("categoryId"), 
                                        r'\.(?=\d)', '.0'))

    return df


def get_resources(spark):
    """Get resources from Firestore

    Args:
        spark (Spark session): Spark session

    Returns:
        Spark DataFrame: Resources from Firestore
    """
       
    db = firestore.Client()  
    collection_ref = db.collection('resources')

    # Get resources 
    resources = collection_ref.stream()

    # Create a list to hold resources data
    resources_list = []

    # Iterate through resources and extract data
    for resource in resources:
        resource_data = resource.to_dict()
        resources_list.append(resource_data)

    # Create a Spark DataFrame from the extracted data
    df = spark.createDataFrame(resources_list)
    
    return df


def clean_resources(df):
    """Select columns of interest for the report and rename columns
    in resources spark dataframe

    Args:
        df (Spark Dataframe): resources obtained from Firestore

    Returns:
        Spark Dataframe: resources ready to join
    """

    # Select columns of interest and rename them
    df = df.select(F.col("id").alias("resourceId"),
                   F.col("providerId"),
                   F.col("promotion"),
                   F.col("categoryId"))

    return df


def get_events(events_bucket_name):
    """Get events from Cloud Storage

    Args:
        events_bucket_name (str): bucket name
 
    Returns:
        Spark Dataframe: events from Storage
    """
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(events_bucket_name)
    blobs = bucket.list_blobs(prefix="events/")
    events = []
    for blob in blobs:
        event_data = blob.download_as_string()
        events.append(json.loads(event_data))
        
    events_schema = StructType([
        StructField("eventId", StringType(), True),
        StructField("eventTime", StringType(), True),
        StructField("processTime", StringType(), True),
        StructField("resourceId", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("countryCode", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("itemPrice", StringType(), True)
    ])

    df = spark.createDataFrame(events, schema = events_schema)
    
    return df


def clean_events(df):

    """Select columns of interest for the report,
    rename columns and transform processTime
    in events spark dataframe

    Args:
        df (Spark Dataframe): events obtained from Storage
        
    Returns:
        Spark Dataframe: events ready to process
    """

    # Extract first 10 characters from the 'processTime' column "yyyy-mm-dd"
    df = df.withColumn("processTime", 
                       F.substring(F.col("processTime"), 1, 10))

    # Select columns of interest and rename them
    df = df.select(F.col("eventId"),  
                   F.col("resourceId"),
                   F.col("countryCode"),
                   F.col("itemPrice"))
    
     # Extract month from 'eventTime' column
    df = df.withColumn("month", 
                       F.substring(F.col("eventTime"), 1, 7))
         
  
    return df


def get_exchange_rate(url):
    response = requests.get(url)
    exchange_rate_data = response.json()
    exchange_rate = exchange_rate_data["exchange_rate"]
    return exchange_rate


def process_data(joined_df):

    """Process joined DataFrame to calculate the amount considering the percentage
    and promotion.

    Args:
        joined_df (Spark DataFrame): Joined DataFrame containing events, resources, 
                                      and categories

    Returns:
        Spark DataFrame: Processed DataFrame with the new "amount" column
    """
    # Calculate the amount by multiplying "itemPrice" and "percent" only when promotion is False
    joined_df = joined_df.withColumn("amount", 
                                      F.when(joined_df["promotion"] == "false", 
                                             F.format_number(joined_df["itemPrice"] * joined_df["percent"] / 100, 2))
                                      .otherwise(0.0))

    return joined_df


def convert_to_usd(row):
    code = row["Code"]
    amount = row["amount"]
    if amount is None:
        return None
    if code == 'USD':
        return row.asDict()  # Convertir la fila a un diccionario y devolverlo directamente
    if code not in exchange_rate:
        return None  
    new_price = exchange_rate[code] * float(amount)
    # Formatear el nuevo precio con dos decimales
    new_price = "{:.2f}".format(new_price)
    # Crear un nuevo diccionario con los datos actualizados y devolverlo
    updated_row = row.asDict()
    updated_row["Code"] = 'USD'
    updated_row["amount"] = new_price
    return updated_row
# -------------------- START SPARK SESSION ------------------------------------

spark = SparkSession.builder.master("local")\
    .appName("Royalties")\
    .getOrCreate()
# ------------------------------- GET DATA  -----------------------------------
# Get categories from URL
categories_url_df = get_categories(spark, categories_url)
categories_df = clean_categories(categories_url_df)

# Get resources from Firestore
resources_firestore_df = get_resources(spark)
resources_df = clean_resources(resources_firestore_df)

# Get events from Storage
events_storage_df = get_events(spark)
events_df = clean_events(events_storage_df)

# Obtener la tasa de cambio de USD
exchange_rate = get_exchange_rate(exchange_rates_url)
#obtener countries 
countries_df=get_countries(spark,countries_url)
#--------------------------- PROCESS DATA -------------------------------------

# Register table alias to allow SQL use
events_df.createOrReplaceTempView("events") 
resources_df.createOrReplaceTempView("resources")
categories_df.createOrReplaceTempView("categories")
countries_df.createOrReplaceTempView("countries")

# Join events with resources to get resource names and category id
joined_df = events_df\
    .join(resources_df, 
         on="resourceId", 
         how="left")

# Join with categories to get category name
joined_df = joined_df\
    .join(categories_df, 
          on="categoryId", 
          how="left")

## Unir el DataFrame de eventos con el DataFrame de países
joined_df = joined_df.join(countries_df, joined_df.countryCode == countries_df.CountryCode, "left")



#Calculating inside processdata function the amount
joined_df = process_data(joined_df)
#Make usd conversion in convert_to_usd function
#Make usd conversion in convert_to_usd function
converted_df = joined_df.rdd.map(lambda row: convert_to_usd(row, exchange_rate)).filter(lambda x: x is not None).toDF(joined_df.schema)

#Select the report columns
selected_df = converted_df.select("month", "providerId", "resourceId", "amount")
# Renombrar la columna "month" a "date"
selected_df = selected_df.withColumnRenamed("month", "date")
selected_df.show(5)

# Write the DataFrame in JSON format with one object per line to Cloud Storage
#converted_df.write.json("gs://tu_bucket/tu_ruta/datos.json", mode="overwrite", lineSep="\n")
# Guardar el DataFrame en formato JSON por líneas en Google Cloud Storage
selected_df.write.json("gs://"+reports_bucket_name+"/datos.jsonl", mode="overwrite", lineSep="\n")

#Execute once per month we have to create a topic relating this script, creating the cloud function and then the job that will execute once per month

