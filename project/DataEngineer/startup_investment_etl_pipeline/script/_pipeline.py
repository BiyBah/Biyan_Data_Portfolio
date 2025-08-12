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
from _staging_pipeline import StagingPipeline
from _warehouse_pipeline import WarehousePipeline

if __name__ == "__main__":
    StagingPipeline.start()
    WarehousePipeline.start()