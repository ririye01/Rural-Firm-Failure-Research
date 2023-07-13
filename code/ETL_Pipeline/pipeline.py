import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame

from typing import Union, List, Tuple, Dict
import multiprocessing

from bronze_to_silver.texas_comptrollers_office.franchise_taxholder \
    import retrieve_franchise_taxholder_df



def _create_spark_session() -> SparkSession:
    NUM_THREADS: int = multiprocessing.cpu_count()
    spark: SparkSession = SparkSession.builder \
                                      .master(f"local[{str(NUM_THREADS)}]") \
                                      .appName("predicting_texas_firm_failure") \
                                      .getOrCreate()
    return spark


def get_empty_spark_dataframe(
    spark: Union[SparkSession, None] = None,
    schema: Union[str, None] = None,
) -> DataFrame:
    """
    If neither a spark session nor schema are passed in, 
        create a spark session, and 
        return an empty spark DataFrame with an empty schema.
    If only a schema is passed in, 
        create a spark session, and
        return an empty spark DataFrame with a predefined schema.
    If only a spark session is passed in,
        return an empty spark DataFrame with an empty schema.
    If both a spark session and schema are passed in,
        return an empty spark DataFrame with a predefined schema.

    Parameters
    ----------
    spark
        SparkSession
    schema
        string
        schema = "col1 STRING, col2 INT, col3 DOUBLE"  
            - Replace with your desired column names and data types
    
    Return
    ------
    spark dataframe
        pyspark.sql.DataFrame
    """
    
    if not (isinstance(spark, SparkSession) or isinstance(schema, str)): # ~(p \/ q)
        spark: SparkSession = _create_spark_session()
        return spark.createDataFrame(
            spark.sparkContext.emptyRDD(), 
            spark.emptyDataFrame.schema,
        )
    elif not isinstance(spark, SparkSession):
        spark: SparkSession = _create_spark_session()
        return spark.createDataFrame(
            spark.sparkContext.emptyRDD(),
            schema,
        )
    elif not isinstance(schema, str):
        return spark.createDataFrame(
            spark.sparkContext.emptyRDD(), 
            spark.emptyDataFrame.schema,
        )
    elif isinstance(spark, SparkSession) and isinstance(schema, str):
        return spark.createDataFrame(
            spark.sparkContext.emptyRDD(),
            schema,
        )


def main() -> None:
    # Create spark session
    spark = _create_spark_session()

    # Retrieve franchise taxholder dataframe and convert it to firm 
    franchise_taxholder_df: DataFrame = retrieve_franchise_taxholder_df(spark)
    

    # End spark session
    spark.stop()


if __name__ == "__main__":
    main()