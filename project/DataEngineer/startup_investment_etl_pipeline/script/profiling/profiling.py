from typing import List, Dict, Optional, Any, Union
import os
import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    min as _min, max as _max, col, countDistinct
)
from pyspark.sql.types import DateType, TimestampType, NumericType
import traceback
from utils.helper import *

class ProfileData():
    @staticmethod
    def table_profiler(spark: SparkSession, table: str, source_type: str = "db", **kwargs) -> List[Dict[str, Any]]:
        """
        Profile a table from database, CSV, or API, returning column statistics including data type,
        null percentage, duplicates, and max/mode.
    
        Args:
            spark: SparkSession object
            table: Name of the table or data source identifier (e.g., table name, file name, or API endpoint)
            source_type: Source type ('db', 'csv', or 'api') (default: 'db')
            **kwargs: Additional parameters:
                - For 'db': db_url, connection_properties
                - For 'csv': path (base directory for CSV files)
                - For 'api': api_url, params (optional query parameters)
    
        Returns:
            List of dictionaries containing profiling results for each column
        """
        results = []
        try:
            # Load data based on source type
            if source_type == "db":
                db_url = kwargs.get("db_url")
                connection_properties = kwargs.get("connection_properties")
                if not db_url or not connection_properties:
                    raise ValueError("db_url and connection_properties required for db source")
                df = spark.read.jdbc(url=db_url, table=table, properties=connection_properties)
            
            elif source_type == "csv":
                path = kwargs.get("path", "data/")
                df = spark.read.csv(f"{path}{table}", header=True)
            
            elif source_type == "api":
                api_url = kwargs.get("api_url")
                params = kwargs.get("params", {})
                if not api_url:
                    raise ValueError("api_url required for api source")
                response = requests.get(url=api_url, params=params)
                if response.status_code != 200:
                    raise Exception(f"API request fail with status code: {response.status_code}")
                json_data = response.json()
                if not json_data:
                    print(f"No data from API for {table}")
                    return results
                df = spark.createDataFrame(pd.DataFrame(json_data))
            
            else:
                raise ValueError(f"Unsupported source_type: {source_type}")
    
            # Get basic stats
            num_rows = df.count()
            num_cols = len(df.columns)
            
            # Profile each column
            for column in df.columns:
                try:
                    # Ensure column name is a string
                    column = str(column)
                    
                    col_type = df.schema[column].dataType
                    col_type_str = str(col_type.simpleString())
                    
                    # Null percentage
                    null_count = df.filter((df[column].isNull()) | (df[column]=="")).count()
                    null_pct = (null_count / num_rows * 100) if num_rows > 0 else 0.0
                    
                    # Duplicate count
                    distinct_count = df.select(countDistinct(col(column))).first()[0]
                    duplicate_count = num_rows - distinct_count if num_rows > 0 else 0
                    
                    # Min and max for numeric/date or mode for others
                    if isinstance(col_type, (DateType, TimestampType, NumericType)):
                        max_val = df.select(_max(col(column))).first()[0]
                        max_val = str(max_val) if max_val is not None else None
                        
                        min_val = df.select(_min(col(column))).first()[0]
                        min_val = str(min_val) if min_val is not None else None

                        mode_val = None
                    else:
                        mode_val = df.groupBy(col(column)).count().orderBy(col("count").desc()).first()
                        mode_val = str(mode_val[column]) if mode_val is not None else None
                        
                        max_val = None
                        min_val = None

                    # Possible data value
                    distinct_value = [row[column] for row in df.select(col(column)).distinct().collect()]
                    
                    # Append results
                    results.append({
                        "PIC": "Biyan",
                        "date": datetime.now(),
                        "table_name": table,
                        "column_name": column,
                        "num_rows": num_rows,
                        "num_columns": num_cols,
                        "data_type": col_type_str,
                        "null_percentage": null_pct,
                        "duplicate_count": duplicate_count,
                        "min": min_val,
                        "max": max_val,
                        "mode": mode_val,
                        "distinct_value": distinct_value
                    })
                
                except Exception as e:
                    print(f"Error profiling column {column} in table {table}: {str(e)}\n{traceback.format_exc()}")
                    continue
        
        except Exception as e:
            print(f"Error profiling table {table} from {source_type}: {str(e)}\n{traceback.format_exc()}")
        
        return results
    
    @classmethod
    def from_database(cls, spark: SparkSession) -> pd.DataFrame:
        """
        Profile all tables in a PostgreSQL database, including column count, row count,
        and per-column statistics (data type, null percentage, duplicates, max/mode).
    
        Args:
            spark: SparkSession object
    
        Returns:
            Pandas DataFrame with profiling results
        """
        # Initialize results list
        results = []
        
        try:
            # Get database connection details
            db_url, db_user, db_pass, db_name = source_engine()
            connection_properties = {
                "user": db_user,
                "password": db_pass,
                "driver": "org.postgresql.Driver"
            }
        
            # Get list of tables
            tables_query = f"(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') AS tables"
            tables_df = spark.read.jdbc(url=db_url, table=tables_query, properties=connection_properties)
            tables = [row.table_name for row in tables_df.collect()]
    
            for table in tables:
                # Profile each table
                res = cls.table_profiler(
                    spark=spark,
                    table=table,
                    source_type='db',
                    db_url=db_url,
                    connection_properties=connection_properties
                )
                results = results + res
    
            # Convert to pandas DataFrame
            result_df = pd.DataFrame(results)
            
        except Exception as e:
            print(f"Error profiling database: {str(e)}\n{traceback.format_exc()}")
            result_df = pd.DataFrame()
    
        finally:
            return result_df
    
    @classmethod
    def from_csv(cls, spark: SparkSession) -> pd.DataFrame:
        """
        Profile all CSV files in a directory, returning column statistics as a DataFrame.

        Args:
            spark: SparkSession object
        
        Returns:
            Pandas DataFrame with profiling results
        """
        path = "data/"
        results = []
        
        try:
            for file_name in os.listdir(path):
                fullpath = os.path.join(path, file_name)

                _, file_extension = os.path.splitext(file_name)
                    
                if file_extension.lower() == '.csv':                  
                    # Start profiling table
                    res = cls.table_profiler(
                        spark=spark,
                        table=file_name,
                        source_type="csv",
                        path=path
                    )
                    results = results + res
    
            result_df = pd.DataFrame(results)
            
        except Exception as e:
            print(f"Error profiling csv: {str(e)}\n{traceback.format_exc()}")
            result_df = pd.DataFrame()
            
        finally:
            return result_df
    
    @classmethod
    def from_api(cls, spark: SparkSession, api_url: str, params: Dict) -> pd.DataFrame:
        """
        Profile data from an API endpoint, returning column statistics as a DataFrame.
        
        Args:
            spark: SparkSession object
            api_url: API endpoint URL
            params: Dictionary of API query parameters
        
        Returns:
            Pandas DataFrame with profiling results        
        """
        try:
            # Start profiling table
            res = cls.table_profiler(
                spark=spark,
                table=api_url,
                source_type="api",
                api_url=api_url,
                params=params
            )
    
            result_df = pd.DataFrame(res)
            
        except Exception as e:
            print(f"Error profiling api: {str(e)}\n{traceback.format_exc()}")
            result_df = pd.DataFrame()
            
        finally:
            return result_df