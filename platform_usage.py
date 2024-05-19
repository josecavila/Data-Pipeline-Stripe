from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from google.cloud import storage
import json

"""
Generates two monthly reports about platform usage by resource:  
one by country and one by time zone. 
Stores them in Cloud Storage 
in Parquet
"""

# ------------------------------ SOME DATA ------------------------------------

reports_bucket_name = "usage-platform-report-big-3 "
events_bucket_name = "events-stripe-big-3"


# --------------------------- SOME FUNCTIONS ----------------------------------

def get_events(spark, events_bucket_name):
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
    extract month and timeZone to columns
    in events spark dataframe

    Args:
        df (Spark Dataframe): events obtained from Storage
        
    Returns:
        Spark Dataframe: events ready to process
    """

    # Select columns of interest 
    df = df.select(F.col("eventId"), 
                   F.col("eventTime"), 
                   F.col("resourceId"),
                   F.col("countryCode"),
                   F.col("duration"))

    # Extract month from 'eventTime' column
    df = df.withColumn("month", 
                       F.substring(F.col("eventTime"), 1, 7))
        
    # Extract timezone from 'eventTime' column
    df = df.withColumn("timeZone", 
                       F.substring(F.col("eventTime"), 20, 6))
  
    return df


#------------------------- START SPARK SESSION --------------------------------

spark = SparkSession.builder.master("local")\
    .appName("platform_usage")\
    .getOrCreate()



# ------------------------------- GET DATA  -----------------------------------

# Get events from Storage
events_storage_df = get_events(spark, events_bucket_name)
events_df = clean_events(events_storage_df)


#--------------------------------- UDFs ---------------------------------------

# UDF to calculate resource total usage vs. all resources total usage in % 
spark.udf.register('usage_percent_total_udf', 
                   lambda total_duration_resource, total_duration_all: 
                        (total_duration_resource / total_duration_all) * 100, 
                   DoubleType())




#--------------------------- PROCESS DATA -------------------------------------

# Register table alias to allow SQL use
events_df.createOrReplaceTempView("events")

# Sum duration by month and resourceId -> totalDurationResource
grouped_df = events_df\
    .groupBy("month", "resourceId")\
    .agg(F.sum("duration").alias("totalDurationResource"))

# Join the DataFrame with the totalDurationResource column
monthly_resources_usage_df = events_df\
    .join(grouped_df, 
          on=["month", "resourceId"], 
          how="left")\



# Sum all totalDurationResource by month -> totalDurationAll
total_duration_all_df = monthly_resources_usage_df\
    .groupBy("month")\
    .agg(F.sum("duration").alias("totalDurationAll"))

# Join the DataFrame with the totalDurationAll column
monthly_resources_usage_df = monthly_resources_usage_df.\
    join(total_duration_all_df, 
         on="month", 
         how="left")
    

# Sum duration by month, country and resourceId -> totalDurationResourceByCountry
grouped_country_df = monthly_resources_usage_df\
    .groupBy("month", "countryCode", "resourceId")\
    .agg(F.sum("duration").alias("totalDurationResourceByCountry"))

# Join the DataFrame with the totalDurationResourceByCountry column
monthly_resources_usage_df = monthly_resources_usage_df\
    .join(grouped_country_df, 
          on=["month", "countryCode", "resourceId"], 
          how="left")
    

# Sum all totalDurationResource by month and country -> totalDurationResourceAllByCountry
grouped_country_all_df = monthly_resources_usage_df\
    .groupBy("month", "countryCode")\
    .agg(F.sum("duration").alias("totalDurationResourceAllByCountry"))

# Join the DataFrame with the totalDurationResourceAllByCountry column
monthly_resources_usage_df = monthly_resources_usage_df\
    .join(grouped_country_all_df, 
          on=["month", "countryCode"], 
          how="left")


# Sum duration by month, timeZone and resourceId -> totalDurationResourceByZone
grouped_zone_df = monthly_resources_usage_df\
    .groupBy("month", "timeZone", "resourceId")\
    .agg(F.sum("duration").alias("totalDurationResourceByzone"))

# Join the DataFrame with the totalDurationResourceByZone column
monthly_resources_usage_df = monthly_resources_usage_df\
    .join(grouped_zone_df, 
          on=["month", "timeZone", "resourceId"], 
          how="left")


# Sum all totalDurationResource by month and time zone -> totalDurationResourceAllByZone
grouped_zone_all_df = monthly_resources_usage_df\
    .groupBy("month", "timeZone")\
    .agg(F.sum("duration").alias("totalDurationResourceAllByZone"))

# Join the DataFrame with the totalDurationResourceAllByCountry column
monthly_resources_usage_df = monthly_resources_usage_df\
    .join(grouped_zone_all_df, 
          on=["month", "timeZone"], 
          how="left")



#--------------------------- ITERATION BY MONTH -------------------------------

# Register table alias to allow SQL use
monthly_resources_usage_df.createOrReplaceTempView("usage")

# Collect distinct months into a list
month_list = [row.month for row in monthly_resources_usage_df.select("month").distinct().collect()]

for one_month in month_list:

    # Prepare monthly report by country
    output_country_df = spark\
        .sql(f"SELECT month,\
                resourceId,\
                countryCode,\
                usage_percent_total_udf(totalDurationResource,totalDurationAll) AS usagePercentTotal,\
                usage_percent_total_udf(totalDurationResourceByCountry, totalDurationResourceAllByCountry) AS usagePercentRelativeCountry,\
                totalDurationResource AS totalDurationInSec \
                FROM usage \
                WHERE usage.month = '{one_month}'")
    
    # Prepare monthly report by time zone
    output_zone_df = spark\
        .sql(f"SELECT month,\
                    resourceId,\
                    timeZone,\
                    usage_percent_total_udf(totalDurationResource, totalDurationAll) AS usagePercentTotal,\
                    usage_percent_total_udf(totalDurationResourceByZone, totalDurationResourceAllByZone) AS usagePercentRelativeTz,\
                    totalDurationResource AS totalDurationInSec \
                FROM usage \
                WHERE usage.month = '{one_month}'")
    
    
    # Drop duplicates
    output_country_df = output_country_df\
        .dropDuplicates(['month', 'resourceId', 'countryCode'])
    
    output_zone_df = output_zone_df\
        .dropDuplicates(['month', 'resourceId', 'timeZone'])
        

    # Two monthly reports are generated
    report_file_name_country = f'usage_country_{one_month}_report.parquet'
    output_country_df.write.mode("overwrite")\
        .parquet(f"gs://{reports_bucket_name}/country/{report_file_name_country}")

    report_file_name_zone = f'usage_zone_{one_month}_report.parquet'
    output_zone_df.write.mode("overwrite")\
        .parquet(f"gs://{reports_bucket_name}/timezone/{report_file_name_zone}")
    


# ------------------------------ STOP SPARK SESSION ---------------------------

spark.stop()
