import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame

from typing import Union, List, Tuple, Dict
import multiprocessing

from bronze_to_silver.texas_comptrollers_office.franchise_taxholder import retrieve_franchise_taxholder_df
from bronze_to_silver.texas_comptrollers_office.firm_growth import (
    convert_franchise_tax_data_to_firm_growth_by_city_and_year,
)


def create_spark_session() -> SparkSession:
    NUM_THREADS: int = multiprocessing.cpu_count()
    spark: SparkSession = (
        SparkSession.builder.master(f"local[{str(NUM_THREADS)}]")
        .appName("predicting_texas_firm_failure")
        .config("spark.driver.memory", "8g")  # Sets the Spark driver memory to 8GB
        .config("spark.executor.memory", "4g")  # Sets the executor memory to 4GB
        .config("spark.sql.shuffle.partitions", "100")  # Sets the number of partitions for shuffling
        .getOrCreate()
    )
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
    schema: str
        Schema follows the pattern "col1 STRING, col2 INT, col3 DOUBLE".
        Replace with your desired column names and data types

    Return
    ------
    pyspark.sql.DataFrame
        pyspark.sql.DataFrame
    """

    if not (isinstance(spark, SparkSession) or isinstance(schema, str)):  # ~(p \/ q)
        spark: SparkSession = create_spark_session()
        return spark.createDataFrame(
            spark.sparkContext.emptyRDD(),
            spark.emptyDataFrame.schema,
        )
    elif not isinstance(spark, SparkSession):
        spark: SparkSession = create_spark_session()
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


def gather_data_sources(spark: SparkSession) -> List[DataFrame]:
    """
    Retrieve data from all relevant API's if data has not already been gathered
    from various sources.

    Parameters
    ----------
    spark: SparkSession
        Active Spark Session

    Returns
    -------
    List[pyspark.sql.DataFrame]
        Contains all datasets
    """
    # Retrieve franchise taxholder dataframe and convert it to firm growth & firm failure
    franchise_taxholder_df: DataFrame = retrieve_franchise_taxholder_df(spark)
    firm_growth_df: DataFrame = convert_franchise_tax_data_to_firm_growth_by_city_and_year(
        old_df=franchise_taxholder_df,
        save_to_csv=True,
    )

    return [firm_growth_df]


def main() -> None:
    # Create spark session
    spark = create_spark_session()

    # Pull datasources from API's
    dfs: List[DataFrame] = gather_data_sources(spark)

    # JOIN Datasources to a new DataFrame
    ### TBD

    # End spark session
    spark.stop()


if __name__ == "__main__":
    main()
