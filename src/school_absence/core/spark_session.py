"""
This module contains functions to initialize and stop a Spark session.
"""
from pyspark.sql import SparkSession

def initialize_spark(app_name="School Absence Analysis", log_level="ERROR"):
    print(f"Initializing Spark session: {app_name}")
    spark = (
        SparkSession.builder
            .master("local[*]")
            .appName(app_name)
            .getOrCreate()
    )
    
    # set log level
    spark.sparkContext.setLogLevel(log_level)
    
    print("Spark session initialized successfully")
    return spark

def stop_spark(spark):
    if spark is not None:
        print("Stopping Spark session")
        spark.stop()