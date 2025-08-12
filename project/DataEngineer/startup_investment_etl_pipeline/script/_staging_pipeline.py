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
from extract import Extract
from load import Load

class StagingPipeline():
    @staticmethod
    def start():
        spark = SparkSession.builder \
                .appName("StagingPipeline") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .getOrCreate()

        # ----------------------------------------------------------------- #
        # EXTRACT FROM SOURCE
        # 1. Extract from source database
        db_table_to_extract = [
            "company",
            "acquisition",
            "ipos",
            "funds",
            "funding_rounds",
            "investments",
        ]
        src_db_dict = Extract.from_database(
            spark=spark,
            table_to_extract=db_table_to_extract, 
            source_type="source"
        )  
        # 2. Extract from source csv                 
        src_csv_dict = Extract.from_csv(
            spark=spark
        )
                
        # 3. Extract from source API
        src_api_dict = {}
        src_api_dict["milestones"] = Extract.from_api(
            spark=spark, 
            start_date="2005-01-01", 
            end_date="2011-01-01"
        )

        src_dict = {**src_db_dict, **src_csv_dict, **src_api_dict}

        # ----------------------------------------------------------------- #
        # LOAD TO STAGING
        # 4. Load to staging database
        Load.to_staging(
            spark=spark, 
            df_dict=src_dict
        )
        spark.stop()