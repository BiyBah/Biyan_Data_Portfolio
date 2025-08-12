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

class Load():
    @staticmethod
    def to_staging(spark: SparkSession, df_dict):
        process = "load_to_staging"
        try:
            # Define current timestamp for logging
            current_timestamp = datetime.now()

            # Define connection properties
            db_url, db_user, db_pass, db_name = stg_engine()
            properties = {
                "user": db_user,
                "password": db_pass
            }
    
            for table_name, spark_df in df_dict.items():
                # Check if any dataframe row count = 0
                if spark_df.count() == 0:
                    raise ValueError(f"Dataframe in table: {table_name} is empty")

                # load data
                spark_df.write.jdbc(url = db_url,
                            table = table_name,
                            mode = "overwrite",
                            properties = properties)
            
            # Structure log message
            error_msg = ""
            status = "success"
                
        except Exception as e:
            # Structure log message
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            Fail to perform {process} for table '{table_name}'.
            
            Full Traceback:
            {tb_str}
            """
            status = "fail"
            
        finally:
            # log message
            log_msg = spark.sparkContext\
                .parallelize([("staging", process, status, "source transformation result", "", current_timestamp, error_msg)])\
                .toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])
         
            load_log(spark, log_msg)

    @staticmethod
    def to_warehouse(spark: SparkSession, df_dict):
        current_timestamp = datetime.now()
        db_url, db_user, db_pass, db_name = wh_engine()
        properties = {
            "user": db_user,
            "password": db_pass
        }
        
        for table_name, df in df_dict.items():
            if table_name == "startup_event":
                wh_table_name = f"fct_{table_name}"
            else:
                wh_table_name = f"dim_{table_name}"
                
            try:
                # truncate table with sqlalchemy
                conn = wh_engine_sqlalchemy()
        
                with conn.connect() as connection:
                    # Execute the TRUNCATE TABLE command
                    quoted_table_name = quoted_name(wh_table_name, quote=True)
                    connection.execute(text(f"TRUNCATE TABLE {quoted_table_name} RESTART IDENTITY CASCADE"))
                    connection.commit()
                conn.dispose()
                
            except Exception as e:
                log_msg = spark.sparkContext\
                    .parallelize([("warehouse", "load", "fail", "validation passed tables", table_name, current_timestamp, str(e))])\
                    .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
                load_log(spark, log_msg)
            
            try:
                # load data               
                df.write.jdbc(url = db_url,
                            table = wh_table_name,
                            mode = "append",
                            properties = properties)
                
                #log message
                log_msg = spark.sparkContext\
                    .parallelize([("warehouse", "load", "success", "validation passed tables", table_name, current_timestamp)])\
                    .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
                
            except Exception as e:
                
                # log message
                log_msg = spark.sparkContext\
                    .parallelize([("warehouse", "load", "fail", "validation passed tables", table_name, current_timestamp, str(e))])\
                    .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
            
            finally:
                load_log(spark, log_msg)
            
 