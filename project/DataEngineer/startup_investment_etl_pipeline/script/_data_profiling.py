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
from profiling import ProfileData

if __name__ == "__main__":
    profile_data_spark = SparkSession \
                        .builder \
                        .appName("profile_data") \
                        .getOrCreate()

    profiling_result_path = "profiling_result/"

    # Profile all table in source database
    profile_db_df = ProfileData.from_database(profile_data_spark)
    profile_db_df.to_csv(f"{profiling_result_path}profile_db.csv")

    # Profile all table in csv directory
    profile_csv_df = ProfileData.from_csv(profile_data_spark)
    profile_csv_df.to_csv(f"{profiling_result_path}profile_csv.csv")

    # Profile table obtained from API
    params = {
        "start_date":"2005-01-01",
        "end_date":"2011-01-01"
    }

    profile_api_df = ProfileData.from_api(profile_data_spark, 
                                        api_url=API_PATH, 
                                        params=params)

    profile_api_df.to_csv(f"{profiling_result_path}profile_api.csv")
    
    # Stop the spark sesion
    profile_data_spark.stop()



