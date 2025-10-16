"""
This module contains the function to load the dataset.
"""
from pyspark.sql import SparkSession, DataFrame

from school_absence.config import Config

def load_dataset(spark: SparkSession, data_path = None) -> DataFrame:
    try:
        if not data_path:
            df = spark.read.parquet(Config.parquet_cache_file)
            print("No data path provided. Dataset loaded form cached.")
            return df
        
        df = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv(data_path)
        print(f"Dataset loaded from {data_path}.")

        df.write.mode("ignore").parquet(Config.parquet_cache_file)
        print(f"Dataset cached to {Config.parquet_cache_file}.")
        return df
    
    except Exception as e:
        print(f"Error loading data set: {str(e)}")
        raise Exception("Error loading data set")