from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from google.cloud import storage
from google.cloud import firestore
import requests
import json

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

categories_url = "https://singularity.luisbelloch.es/v1/stripe/categories"
reports_bucket_name = "top-10-resources-diary-report-big-3"
events_bucket_name = "events-stripe-big-3"


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
                   F.col("name").alias("categoryName"))
    
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
                   F.col("name").alias("resourceName"), 
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
                   F.col("processTime").alias("date"), 
                   F.col("resourceId"))
  
    return df



# -------------------- START SPARK SESSION ------------------------------------

spark = SparkSession.builder.master("local")\
    .appName("top10")\
    .getOrCreate()
    
    
# --------------------------- GET DATA  ---------------------------------------

# Get categories from URL
categories_url_df = get_categories(spark, categories_url)
categories_df = clean_categories(categories_url_df)

# Get resources from Firestore
resources_firestore_df = get_resources(spark)
resources_df = clean_resources(resources_firestore_df)

# Get events from Storage
events_storage_df = get_events(spark)
events_df = clean_events(events_storage_df)

 

# ----------------------------- PROCESS DATA ----------------------------------

# Register table alias to allow SQL use
events_df.createOrReplaceTempView("events")
resources_df.createOrReplaceTempView("resources")
categories_df.createOrReplaceTempView("categories")

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
    
# Group by date and resourceId, then count occurrences
purchase_count_df = events_df\
    .groupBy("date", "resourceId")\
    .count()
purchase_complete_df = joined_df.join(purchase_count_df, 
                                      on=["date", "resourceId"], 
                                      how="left")

# Rank 10 resources based on purchase count by category and date
windowSpec = Window.partitionBy("date", "categoryId")\
                   .orderBy(F.col("count").desc())
ranked_df = purchase_complete_df\
    .withColumn("position", F.dense_rank().over(windowSpec))\
    .filter(F.col("position") <= 10)\
    .dropDuplicates(['date', 'categoryId', 'resourceId'])\
    .orderBy('date', 'categoryId', 'position')



# --------------------------- GENERATE REPORTS & UPLOAD -----------------------

# Collect distinct dates into a list
date_list = [row.date for row in events_df.select("date").distinct().collect()]

for one_day in date_list:
    # Filter by date
    ranked_df.createOrReplaceTempView("ranked")
    output_df = spark\
        .sql(f"""SELECT position, categoryId, categoryName, resourceId, resourceName 
                 FROM ranked 
                 WHERE ranked.date = '{one_day}'""")

    # One report by day is generated in CSV
    report_file_name = f'top10_{one_day}_report.csv'
    
    # Write DataFrame to CSV format in Cloud Storage
    output_df.write \
        .option("header", "true") \
        .option("sep", "|") \
        .mode("overwrite") \
        .csv(f"gs://{reports_bucket_name}/{report_file_name}")

    

# ------------------------------- STOP SPARK SESSION --------------------------

spark.stop()
