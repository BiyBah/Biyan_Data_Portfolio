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
from transform import Transform
from validate import Validate

class WarehousePipeline():
    @staticmethod
    def start():
        spark = SparkSession.builder \
                .appName("WarehousePipeline") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .getOrCreate()


        # ----------------------------------------------------------------- #
        # EXTRACT FROM STAGING
        # 5. Extract from staging database
        stg_db_tbl = [
            "company",
            "acquisition",
            "ipos",
            "funds",
            "funding_rounds",
            "investments",
            "people",
            "relationships",
            "date",
            "milestones",
            "event_type"
        ]
        src_dict = {}
        src_dict = Extract.from_database(
            spark=spark,
            table_to_extract=stg_db_tbl, 
            source_type="staging"
        )
        # ----------------------------------------------------------------- #
        # TRANSFORM STAGING
        # 6. Transform staging database
        src_dict = Transform.staging(
            spark=spark, 
            df_dict=src_dict
        )

        gc.collect()

        # ----------------------------------------------------------------- #
        # VALIDATE TRANSFORMATION
        # 7. Validate data transformation from staging
        validation_passed = Validate.transform_staging(
            spark=spark,
            df_dict=src_dict
        )

        if not validation_passed:
            print("Validation process fail. There is null in fact table")
        else:
            # ----------------------------------------------------------------- #
            # LOAD TO WAREHOUSE
            # 9. Load data to warehouse
            Load.to_warehouse(
                spark=spark,
                df_dict=src_dict)

        spark.stop()