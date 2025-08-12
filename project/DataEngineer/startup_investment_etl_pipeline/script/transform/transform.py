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

class Transform():
    @staticmethod
    def _hashing(
        df: DataFrame,
        hash_cols: List[str],
        hash_output_colname: str
    ) -> DataFrame:
        # Create UUID using sha256 hash
        # Ensure all hash_cols are cast to string for consistent hashing
        hash_expressions = [col(c).cast("string") for c in hash_cols]
        df = df.withColumn(hash_output_colname, sha2(concat_ws("||", *hash_expressions), 256))
        return df

    @staticmethod
    def _common_transformations(
        src_df: DataFrame,
        nk_mapping: Optional[Dict[str, str]] = None,
        type_mapping: Optional[Dict[str, Union[str, DataType]]] = None,
        literals: Optional[Dict[str, str]] = None,
        drop_cols: Optional[List[str]] = None,
        hash_cols: Optional[List[str]] = None,
        hash_output_colname: Optional[str] = None,
        fk_df: Optional[DataFrame] = None,
        fk_col: Optional[Dict[str, str]] = None,
        select_colname: Optional[List[str]] = None
    ) -> Optional[DataFrame]:
        """Applies renames, type changes, literals, drops, hash creation, foreign key joins, and column selection."""
        # Rename columns
        if nk_mapping:
            for original_col, new_cols in nk_mapping.items():
                if original_col in src_df.columns:
                    # Handle both single string and list of strings
                    if isinstance(new_cols, str):
                        new_cols = [new_cols]
                    
                    for new_col in new_cols:
                        src_df = src_df.withColumn(new_col, col(original_col))
    
        # Modify column data type
        if type_mapping:
            for colname, datatype in type_mapping.items():
                if colname in src_df.columns:
                    src_df = src_df.withColumn(colname, col(colname).cast(datatype))
                        
        # Create literal column
        if literals:
            for colname, literal in literals.items():
                src_df = src_df.withColumn(colname, lit(literal))
    
        # Drop columns
        if drop_cols:
            drop_cols = [c for c in drop_cols if c in src_df.columns]
            src_df = src_df.drop(*drop_cols)
    
        # Create UUID using sha256 hash
        if hash_cols and hash_output_colname:
            hash_cols = [c for c in hash_cols if c in src_df.columns]
            if hash_cols:
                hash_expressions = [coalesce(col(c).cast("string"), lit("null")) for c in hash_cols]
                src_df = src_df.withColumn(hash_output_colname, sha2(concat_ws("||", *hash_expressions), 256))
    
        # Create foreign key
        if fk_df and fk_col:
            for lookup_key, lookup_val in fk_col.items():
                if lookup_key in src_df.columns and lookup_val in fk_df.columns:
                    fk_df_subset = fk_df.select(lookup_key, lookup_val)
                    src_df = src_df.join(fk_df_subset, lookup_key, "left")
    
        # Select columns
        if select_colname:
            select_colname = [c for c in select_colname if c in src_df.columns]
            src_df = src_df.select(*select_colname)
    
        return src_df

    @staticmethod
    def _get_first_key(d, val):
        return next((key for key, value in d.items() if value == val), None)
        
    @classmethod
    def staging(cls, spark: SparkSession, df_dict: Dict[str, DataFrame]) -> Optional[Dict]:
        # Create completely independent DataFrames copy:
        dim = {
            key: df.select("*")
            for key, df in df_dict.items()
        }
        
        # Define current_timestamp for logging
        current_timestamp = datetime.now()
       
        # ----------------------------------------------------------------- #
        # Transform company into dim_company
    
        # office_id -> office_nk int
        # object_id -> object_nk
        # ensure latitude and longitude to be DecimalType(9,6)
        table_name = "company"
        hash_cols = ["office_nk", "object_nk", "description", "region",
                    "address1", "address2", "city", "zip_code",
                    "state_code", "country_code", "latitude", "longitude"]
        try:
            dim["company"] = cls._common_transformations(
                src_df=dim["company"],
                nk_mapping={
                    "office_id": "office_nk", 
                    "object_id": "object_nk"
                },
                type_mapping={
                    "latitude": DecimalType(9,6),
                    "longitude": DecimalType(9,6)
                },
                hash_cols=hash_cols,
                hash_output_colname="company_id",
                select_colname=["company_id"] + hash_cols
            )
            
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
            
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform acquisition into dim_acquisition

        # acquisition_id -> acquisition_nk
        # acquiring_object_id -> acquiring_object_nk
        # acquired_object_id -> acquired_object_nk
        
        table_name = "acquisition"
        hash_cols =["acquisition_nk", "acquiring_object_nk", "acquired_object_nk", 
                    "term_code", "price_amount", "price_currency_code"]
        try:
            dim["acquisition"] = cls._common_transformations(
                src_df=dim["acquisition"],
                nk_mapping={
                    "acquisition_id": "acquisition_nk", 
                    "acquiring_object_id": "acquiring_object_nk",
                    "acquired_object_id": "acquired_object_nk"
                },
                hash_cols=hash_cols,
                hash_output_colname="acquisition_id",
                select_colname=["acquisition_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
            
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform ipos into dim_ipos

        # ipo_id -> ipo_nk
        # object_id -> object_nk
        table_name = "ipos"
        hash_cols = ["ipo_nk", "valuation_amount", 
                    "valuation_currency_code", "raised_amount",
                     "raised_currency_code", "stock_symbol"]
        try:
            dim["ipos"] = cls._common_transformations(
                src_df=dim["ipos"],
                nk_mapping={
                    "ipo_id": "ipo_nk", 
                    "object_id": "object_nk"
                },
                hash_cols=hash_cols,
                hash_output_colname="ipos_id",
                select_colname=["ipos_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
            
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform funds into dim_funds

        # fund_id -> fund_nk
        # object_id -> object_nk
        table_name = "funds"
        hash_cols = ["fund_nk", "name", 
                    "raised_amount", "raised_currency_code"]
        try:
            dim["funds"] = cls._common_transformations(
                src_df=dim["funds"],
                nk_mapping={
                    "fund_id": "fund_nk", 
                    "object_id": "object_nk"
                },
                hash_cols=hash_cols,
                hash_output_colname="funds_id",
                select_colname=["funds_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform funding_rounds into dim_funding_rounds

        # funding_round_id -> funding_round_nk
        # object_id -> object_nk
        table_name = "funding_rounds"
        hash_cols=["funding_round_nk", "funding_round_type", 
                    "funding_round_code", "raised_amount_usd", "raised_amount",
                    "raised_currency_code", "pre_money_valuation_usd", "pre_money_valuation",
                    "pre_money_currency_code", "post_money_valuation_usd", "post_money_valuation", 
                    "post_money_currency_code", "participants", "is_first_round", "is_last_round",
                    "created_by"]
        try:
            dim["funding_rounds"] = cls._common_transformations(
                src_df=dim["funding_rounds"],
                nk_mapping={
                    "funding_round_id": "funding_round_nk", 
                    "object_id": "funded_object_nk"
                },
                hash_cols=hash_cols,
                hash_output_colname="funding_rounds_id",
                select_colname=["funding_rounds_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform investments into dim_investments

        # funding_round_id -> funding_round_nk
        # object_id -> object_nk
        # Create foreign key using funding_rounds_id from funding_rounds
        table_name = "investments"
        hash_cols=["funding_rounds_id", "investment_nk", "funding_round_nk",
                    "funded_object_nk", "investor_object_nk"]
        try: 
            dim["investments"] = cls._common_transformations(
                src_df=dim["investments"],
                nk_mapping={
                    "investment_id": "investment_nk", 
                    "funding_round_id": "funding_round_nk",
                    "funded_object_id": "funded_object_nk",
                    "investor_object_id": "investor_object_nk"
                },
                fk_df=dim["funding_rounds"],
                fk_col={"funding_round_nk":"funding_rounds_id"},
                hash_cols=hash_cols,
                hash_output_colname="investments_id",
                select_colname=["investments_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform people into dim_people

        # people_id -> people_nk
        # object_id -> object_nk
        table_name = "people"
        hash_cols=["people_nk", "first_name", 
                   "last_name", "birthplace", "affiliation_name"]
        try:
            dim["people"] = cls._common_transformations(
                src_df=dim["people"],
                nk_mapping={
                    "people_id": "not_used",
                    "object_id": "people_nk", 
                },
                hash_cols=hash_cols,
                hash_output_colname="people_id",
                select_colname=["people_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform relationships into dim_relationships

        # relationship_id -> relationship_nk
        # person_object_id -> person_object_nk
        # relationship_object_id -> relationship_object_nk
        # ensure data type is correct for
        ## start_at (timestamp), end_at (timestamp), is_past (bool) and sequence (integer)
        # Create foreign key: people_id -> dim_people, company_id -> company
        table_name = "relationships"
        hash_cols=["people_id", "company_id",
                    "relationship_nk", "person_object_nk", 
                    "relationship_object_nk", 
                    "start_at", "end_at", "is_past",
                    "sequence", "title"]
        try:
            dim["relationships"] = cls._common_transformations(
                src_df=dim["relationships"],
                nk_mapping={
                    "relationship_id": "relationship_nk", 
                    "person_object_id": "people_nk",
                    "relationship_object_id": "object_nk"
                },
                type_mapping={
                    "start_at":"timestamp",
                    "end_at":"timestamp",
                    "is_past":"boolean",
                    "sequence":"int"
                },
                fk_df=dim["people"],
                fk_col={"people_nk": "object_nk"}
            )
            dim["relationships"] = cls._common_transformations(
                src_df=dim["relationships"],
                fk_df=dim["company"],
                fk_col={"object_nk":"company_id"}
            )
            dim["relationships"] = cls._common_transformations(
                src_df=dim["relationships"],
                nk_mapping={
                    "people_nk": "person_object_nk", 
                    "object_nk": "relationship_object_nk"
                },                
                hash_cols=hash_cols,
                hash_output_colname="relationships_id",
                select_colname=["relationships_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform milestones into dim_milestones

        # milestone_id -> milestone_nk
        # object_id -> object_nk
        table_name = "milestones"
        hash_cols=["milestone_nk", "description", "milestone_code"]
        
        try:
            dim["milestones"] = cls._common_transformations(
                src_df=dim["milestones"],
                nk_mapping={
                    "milestone_id": "milestone_nk", 
                },
                hash_cols=hash_cols,
                hash_output_colname="milestones_id",
                select_colname=["milestones_id"] + hash_cols
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
            
        # ----------------------------------------------------------------- #
        # Transform event_type into dim_event_type
        ## unchanged
        
        # ----------------------------------------------------------------- #
        # Transform date into dim_date
        table_name = "date"
        try:
            dim["date"] = cls._common_transformations(
                src_df=dim["date"],
                type_mapping={
                    "date_id": "int"
                }
            )
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
        
        # ----------------------------------------------------------------- #
        # Define common variable that will be used
        event_type_map = {row.event_type_id: row.event_type 
                          for row in dim["event_type"].select("event_type_id", "event_type").collect()}
        common_colname = ["company_id", "event_type_id",
                          "event_datetime", "event_date",
                          "source_url", "source_description"]
        
        # Transform data into fct_startup_event
        table_name = "startup_event"
        try:
            # ----------------------------------------------------------------- #
            # Event: Acquiring
            fct_acquiring = cls._common_transformations(
                src_df=df_dict["acquisition"].select("*"),
                nk_mapping={
                    "acquisition_id":"acquisition_nk",
                    "acquiring_object_id":"object_nk",
                    "acquired_at":"event_datetime",
                    "acquired_at":"event_date"
                },
                type_mapping={
                    "event_date":"timestamp",
                    "event_date":"date"
                },
                literals={
                    "event_type_id":cls._get_first_key(
                        event_type_map, "Acquiring"
                    )
                },
                drop_cols=["acquisition_id"],
                fk_df=dim["acquisition"],
                fk_col={"acquisition_nk": "acquisition_id"}
            )
            fct_acquiring_final = cls._common_transformations(
                src_df=fct_acquiring,
                fk_df=dim["company"],
                fk_col={"object_nk": "company_id"},
                select_colname=["acquisition_id", "acquisition_nk"] + common_colname
            )
            
            # ----------------------------------------------------------------- #
            # Event: Acquired 
            fct_acquired = cls._common_transformations(
                src_df=df_dict["acquisition"].select("*"),
                nk_mapping={
                    "acquisition_id":"acquisition_nk",
                    "acquired_object_id":"object_nk",
                    "acquired_at":"event_datetime",
                    "acquired_at":"event_date"
                },
                type_mapping={
                    "event_datetime":"timestamp",
                    "event_date":"date"
                },
                literals={
                    "event_type_id":cls._get_first_key(
                        event_type_map, "Acquired"
                    )
                },
                drop_cols=["acquisition_id"],
                fk_df=dim["acquisition"],
                fk_col={"acquisition_nk": "acquisition_id"}
            )
            fct_acquired_final = cls._common_transformations(
                src_df=fct_acquired,
                fk_df=dim["company"],
                fk_col={"object_nk": "company_id"},
                select_colname=["acquisition_id", "acquisition_nk"] + common_colname
            )
            
            # ----------------------------------------------------------------- #
            # Event: IPO 
            fct_ipos = cls._common_transformations(
                src_df=df_dict["ipos"].select("*"),
                nk_mapping={
                    "ipo_id":"ipo_nk",
                    "object_id":"object_nk",
                    "public_at":"event_datetime",
                    "public_at":"event_date"
                },
                type_mapping={
                    "event_date":"timestamp",
                    "event_date":"date"
                },
                literals={
                    "event_type_id":cls._get_first_key(
                        event_type_map, "IPO"
                    )
                },
                fk_df=dim["ipos"],
                fk_col={"ipo_nk": "ipos_id"}
            )
            fct_ipos_final = cls._common_transformations(
                src_df=fct_ipos,
                fk_df=dim["company"],
                fk_col={"object_nk": "company_id"},
                select_colname=["ipos_id", "ipo_nk"] + common_colname
            )  

            # ----------------------------------------------------------------- #
            # Event: Funds 
            fct_funds = cls._common_transformations(
                src_df=df_dict["funds"].select("*"),
                nk_mapping={
                    "fund_id":"fund_nk",
                    "object_id":"object_nk",
                    "funded_at":"event_datetime",
                    "funded_at":"event_date"
                },
                type_mapping={
                    "event_datetime":"timestamp",
                    "event_date":"date"
                },
                literals={
                    "event_type_id":cls._get_first_key(
                        event_type_map, "Funds"
                    )
                },
                fk_df=dim["funds"],
                fk_col={"fund_nk": "funds_id"}
            )
            fct_funds_final = cls._common_transformations(
                src_df=fct_funds,
                fk_df=dim["company"],
                fk_col={"object_nk": "company_id"},
                select_colname=["funds_id", "fund_nk"] + common_colname
            )
            
            # ----------------------------------------------------------------- #
            # Event: Received Funding, Made Investment and Funding Rounds
            fct_received_funding = cls._common_transformations(
                src_df=df_dict["investments"].select("*"),
                nk_mapping={
                    "investment_id":"investment_nk",
                    "funding_round_id":"funding_round_nk",
                    "funded_object_id":"object_nk"
                },
                literals={
                    "event_type_id":cls._get_first_key(
                        event_type_map, "Received Funding"
                    )
                },
                fk_df=dim["company"],
                fk_col={"object_nk": "company_id"},
                select_colname=["investment_nk", "funding_round_nk", "company_id", "event_type_id"]
            )
            fct_made_investment = cls._common_transformations(
                src_df=df_dict["investments"].select("*"),
                nk_mapping={
                    "investment_id":"investment_nk",
                    "funding_round_id":"funding_round_nk",
                    "investor_object_id":"object_nk"
                },
                literals={
                    "event_type_id":cls._get_first_key(
                        event_type_map, "Made Investment"
                    )
                },
                fk_df=dim["company"],
                fk_col={"object_nk": "company_id"},
                select_colname=["investment_nk","funding_round_nk", "company_id", "event_type_id"]
            )
            fct_funding_rounds = cls._common_transformations(
                src_df=df_dict["funding_rounds"].select("*"),
                nk_mapping={
                    "funding_round_id":"funding_round_nk",
                    "funded_at":["event_datetime", "event_date"]
                },
                type_mapping={
                    "event_datetime":"timestamp",
                    "event_date":"date"
                },
                fk_df=dim["funding_rounds"],
                fk_col={"funding_round_nk": "funding_rounds_id"},
                select_colname=["funding_rounds_id", "funding_round_nk", "event_datetime",
                                "event_date", "source_url", "source_description"]
            )
            fct_received_funding_final = fct_received_funding.join(
                fct_funding_rounds,"funding_round_nk","left"
            )
            fct_made_investment_final = fct_made_investment.join(
                fct_funding_rounds,"funding_round_nk","left"
            )
            fct_funding_rounds_final = fct_received_funding_final\
                                           .unionByName(fct_made_investment_final, allowMissingColumns=True)\
                                           .select(["funding_rounds_id", "funding_round_nk", "investment_nk"] + common_colname)
            
            # ----------------------------------------------------------------- #
            # Event: Milestone 
            fct_milestones = cls._common_transformations(
                src_df=df_dict["milestones"].select("*"),
                nk_mapping={
                    "milestone_id":"milestone_nk",
                    "object_id":"object_nk",
                    "milestone_at":"event_datetime",
                    "milestone_at":"event_date"
                },
                type_mapping={
                    "event_datetime":"timestamp",
                    "event_date":"date"
                },
                literals={
                    "event_type_id":cls._get_first_key(
                        event_type_map, "Milestone"
                    )
                },
                fk_df=dim["milestones"],
                fk_col={"milestone_nk": "milestones_id"}
            )
            fct_milestones_final = cls._common_transformations(
                src_df=fct_milestones,
                fk_df=dim["company"],
                fk_col={"object_nk": "company_id"},
                select_colname=["milestones_id", "milestone_nk"] + common_colname
            )   
            
            # ----------------------------------------------------------------- #
            # Union All to become fct_startup_event
            
            # Union with unionByName
            dim["startup_event"] = fct_acquiring_final
            for df in [fct_acquired_final, fct_ipos_final, fct_funds_final, fct_funding_rounds_final, fct_milestones_final]:
                dim["startup_event"] = dim["startup_event"].unionByName(df, allowMissingColumns=True)
            
            # Add startup_event_id
            dim["startup_event"] = cls._common_transformations(
                src_df=dim["startup_event"],
                hash_cols=["company_id", "acquisition_id", "ipos_id",
                           "funds_id", "funding_rounds_id", "milestones_id",
                           "event_type_id", "object_nk", "acquisition_nk",
                           "ipo_nk", "fund_nk", "investment_nk",
                           "funding_round_nk", "event_datetime",
                           "event_date", "source_url", "source_description"
                          ],
                hash_output_colname="startup_event_id"
            )

            # Add stable_id to help upsert later by hashing all natural key
            dim["startup_event"] = cls._common_transformations(
                src_df=dim["startup_event"],
                hash_cols=["object_nk", "acquisition_nk", "ipo_nk", 
                           "fund_nk", "funding_round_nk", "investment_nk",
                           "milestone_nk"],
                hash_output_colname="stable_id",
                select_colname=[
                    "startup_event_id", "stable_id", "acquisition_id", "ipos_id",  
                    "funds_id", "funding_rounds_id", "milestones_id"
                ] + common_colname
            )
            
            # Add created_at and updated_at
            for table_name, _ in dim.items():
                dim[table_name] = dim[table_name]\
                                     .withColumn("created_at", lit(datetime.now()))\
                                     .withColumn("updated_at", lit(datetime.now()))
                        
            # Log success transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "success",
                               "staging extraction result",
                               table_name,
                               current_timestamp)])\
                .toDF(['step',
                       'process',
                       'status',
                       'source',
                       'table_name',
                       'etl_date'])
            load_log(spark, log_msg)
            
        except Exception as e:
            # Capture full traceback information
            tb_str = traceback.format_exc()
            error_msg = f"""
            fail to perform transform operation for table '{table_name}'.
            
            Error Details:
            - Error Type: {type(e).__name__}
            - Error Message: {str(e)}
            - Table: {table_name}
            
            Full Traceback:
            {tb_str}
            """
            # Log fail transformation
            log_msg = spark.sparkContext\
                .parallelize([("warehouse",
                               "transform_staging",
                               "fail",
                               "staging extraction result",
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
            return None
        # ----------------------------------------------------------------- #
        # Return back the dim dictionary
        return dim