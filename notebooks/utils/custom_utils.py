from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, concat, lit, row_number, desc, current_timestamp, trim
)
from delta.tables import DeltaTable
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class transformations:

    def dedup(self,df:DataFrame,dedup_col:List,cdc:str):
        try:
            logger.info(f"Deduplication started on columns {dedup_col}")
            df = df.withColumn("dedupKey",concat(*dedup_col))
            df = df.withColumn("dedupCounts",row_number()\
                    .over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
            df = df.filter(col("dedupCounts") == 1)
            df = df.drop("dedupKey","dedupCounts")
            logger.info(f"Deduplication completed on columns {dedup_col}")
            return df
        except Exception as e:
            logger.error(f"Deduplication failed on columns {dedup_col}: {e}")
            raise
    

    def transform_timestamp(self, df):
        try:
            logger.info("Transforming timestamp")
            df = df.withColumn("process_timestamp",current_timestamp())
            logger.info("Transforming timestamp completed")
            return df
        except Exception as e:
            logger.error(f"Transforming timestamp failed: {e}")
            raise
    
    def upsert(self, spark, df, key_cols, table, cdc):
        try:
            logger.info(f"Starting upsert into silver.{table}")
            merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
            dlt_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")
            dlt_obj.alias("trg").merge(df.alias("src"), merge_condition)\
                                .whenMatchedUpdateAll(condition=f"src.{cdc} >= trg.{cdc}")\
                                .whenNotMatchedInsertAll()\
                                .execute()
            logger.info(f"Upsert into silver.{table} completed successfully")
            return 1
        except Exception as e:
            logger.error(f"Upsert into silver.{table} failed: {e}")
            raise
                     

    def trim_strings(self,df:DataFrame):
        try:
            logger.info("Trimming Strings")
            for column_name, dtype in df.dtypes:
                if dtype == "string":
                    df = df.withColumn(column_name, trim(col(column_name)))
            logger.info("Trimming Strings completed")
            return df
        except Exception as e:
            logger.error(f"Trimming Strings failed: {e}")
            raise

    def handle_nulls(self,df:DataFrame,key_col):
        try:
            logger.info(f"Handling nulls: {key_col}")
            df = df.dropna(subset = [key_col,"last_updated_timestamp"])
            logger.info(f"Handling nulls is completed: {key_col}")
            return df
        except Exception as e:
            logger.error(f"Handling nulls failed: {e}")
            raise
