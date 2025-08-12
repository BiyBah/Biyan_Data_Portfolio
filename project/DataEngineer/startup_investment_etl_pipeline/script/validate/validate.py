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

class Validate():    
    @staticmethod
    def transform_staging(
        spark: SparkSession,
        df_dict: Dict[str, DataFrame],
    ) -> Optional[bool]:
        try:
            current_timestamp = datetime.now()
            
            table_name = "startup_event"
            df = df_dict[table_name]
            
            col_to_check = "company_id"
            
            # Use first() to find any null - stops as soon as one is found
            try:
                first_null = df.filter(col(col_to_check).isNull()).first()
                validation_passed = first_null is None
            except:
                null_count = df.filter(col(col_to_check).isNull()).limit(1).count()
                validation_passed = null_count == 0
                
            if validation_passed:
                # Log success validation
                log_msg = spark.sparkContext\
                    .parallelize([("warehouse",
                                   "validate_transformation",
                                   "success",
                                   "staging_transformation_result",
                                   table_name,
                                   current_timestamp)])\
                    .toDF(['step',
                           'process',
                           'status',
                           'source',
                           'table_name',
                           'etl_date'])
                load_log(spark, log_msg)
            else:
                # Log fail validation
                error_msg = f"Found null values in company_id column of {table_name}"
                log_msg = spark.sparkContext\
                    .parallelize([("warehouse",
                                   "validate_transformation",
                                   "fail",
                                   "staging_transformation_result",
                                   table_name,
                                   current_timestamp,
                                   error_msg)])\
                    .toDF(['step',
                           'process',
                           'status',
                           'source',
                           'table_name',
                           'etl_date',
                           'error_msg'])
                load_log(spark, log_msg)
                
        except Exception as e:
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform validation operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}  
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "validate_transformation",
                               "fail",
                               "staging_transformation_result",
                               table_name,
                               current_timestamp,
                               error_msg)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date',
                       'error_msg'])
            load_log(spark, log_msg)
            return False
            
        return validation_passed