from typing import List, Dict, Optional, Any, Union
from sqlalchemy import create_engine, text
from sqlalchemy.sql import quoted_name
from dotenv import load_dotenv
import os
import stat
import glob
import shutil

import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    sum as _sum, min as _min, max as _max, avg as _avg, sha2, col, when, coalesce, lit, concat_ws, first, countDistinct
)
from pyspark.sql.types import DataType, DateType, TimestampType, NumericType, DecimalType
import traceback
import gc

from utils.helper import *

class Extract():
    @staticmethod
    def from_database(spark: SparkSession, table_to_extract: List, source_type: str = "source", write_log: bool = True) -> Optional[Dict]:
        current_timestamp = datetime.now()
        df_dict = {}
        
        try:
            # Database connection
            if source_type == "source":
                source_db_url, source_db_user, source_db_pass, source_db_name = source_engine()
                step = "staging"
            elif source_type == "staging":
                source_db_url, source_db_user, source_db_pass, source_db_name = stg_engine()
                step = "warehouse"
            else:
                raise ValueError("Unknown source type")
                return None
            
            # Connection properties
            connection_properties = {
                "user": source_db_user,
                "password": source_db_pass,
                "driver": "org.postgresql.Driver"
            }

            # Loop and extract table from table_to_extract
            for table_name in table_to_extract:
                # Read from postgres
                df = spark.read.jdbc(
                    url=source_db_url,
                    table=table_name,
                    properties=connection_properties
                )

                if df.count() == 0:
                    raise ValueError(f"{table_name} is empty")

                df_dict[table_name] = df
                
                # Log extract success
                if write_log:
                    log_msg = spark.createDataFrame(
                        [(step, f"{source_type}_extraction", "success", source_db_name, table_name, current_timestamp)],
                        ['step', 'process', 'status', 'source', 'table_name', 'etl_date']
                    )
                    load_log(spark, log_msg)
            # return dictionary of spark dataframe
            return df_dict
        
        except Exception as e:
            # Log failure
            if write_log:
                log_msg = spark.createDataFrame(
                    [(step, f"{source_type}_extraction", "fail", source_db_name, "", current_timestamp, str(e))],
                    ['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg']
                )
                load_log(spark, log_msg) 
            return None

    @staticmethod
    def from_csv(spark: SparkSession, path = "data/", step="staging", write_log: bool = True) -> Optional[DataFrame]:
        current_timestamp = datetime.now()
        df_dict = {}
        try:
            for file_name in os.listdir(path):
                fullpath = os.path.join(path, file_name)
            
                table_name, file_extension = os.path.splitext(file_name)
                    
                if file_extension.lower() == '.csv': 
                    df = spark.read.csv(f"{path}{file_name}", header=True, inferSchema=True)
                    if df.count() == 0:
                        raise ValueError(f"{table_name} is empty")
                    
                    df_dict[table_name] = df
                    
                    if write_log:
                        log_msg = spark.createDataFrame(
                            [(step, "extraction", "success", f"{path}{file_name}", file_name, current_timestamp)],
                            ['step', 'process', 'status', 'source', 'table_name', 'etl_date']
                        )
                        load_log(spark, log_msg)
                    
            return df_dict
        
        except Exception as e:
            if write_log:
                log_msg = spark.createDataFrame(
                    [(step, "extraction", "fail", f"{path}{file_name}", file_name, current_timestamp, str(e))],
                    ['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg']
                )
                load_log(spark, log_msg)
            
            print(f"Error extracting CSV {file_name}: {str(e)}")
            return None
        
    @staticmethod
    def from_api(spark: SparkSession, start_date: str, end_date: str, write_log: bool = True) -> Optional[DataFrame]:
        """
        Extract data from an API and convert to Spark DataFrame with logging.
        
        Args:
            spark: SparkSession object
            ds: Date string for API query
            write_log: Whether to log the extraction process (default: False)
        
        Returns:
            Spark DataFrame or None if extraction fails
        """
        current_timestamp = datetime.now()
        try:
            response = requests.get(
                url=API_PATH,
                params={"start_date": start_date, "end_date": end_date}
            )
            
            if response.status_code != 200:
                raise Exception(f"API request fail with status code: {response.status_code}")
            
            json_data = response.json()
            if not json_data:
                if write_log:
                    log_msg = spark.createDataFrame(
                        [("staging", "extraction", "skipped", "api", "milestone", current_timestamp, "No new data in API")],
                        ['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg']
                    )
                    load_log(spark, log_msg)
                return None
            
            # Convert JSON to Spark DataFrame
            df = spark.createDataFrame(json_data)
            
            if write_log:
                log_msg = spark.createDataFrame(
                    [("staging", "extraction", "success", "api", "milestone", current_timestamp)],
                    ['step', 'process', 'status', 'source', 'table_name', 'etl_date']
                )
                load_log(spark, log_msg)
            
            return df
        
        except Exception as e:
            if write_log:
                log_msg = spark.createDataFrame(
                    [("staging", "extraction", "fail", "api", "data", current_timestamp, str(e))],
                    ['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg']
                )
                load_log(spark, log_msg)
            
            print(f"Error extracting API data: {str(e)}")
            return None