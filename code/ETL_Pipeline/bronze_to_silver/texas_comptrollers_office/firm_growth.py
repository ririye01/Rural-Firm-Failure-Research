import pyspark
from pyspark.sql import functions as F, DataFrame, SparkSession


def convert_franchise_tax_data_to_firm_growth_by_city(
    spark: SparkSession,
    df: DataFrame,
) -> DataFrame:
    """
    Converted raw franchise taxholder data from the Texas Comptroller's
    Office database to firm growth per year within each city in Texas.

    Parameters
    ----------
    spark: SparkSession
        A SparkSession object to enable the creation of DataFrame.
    df: pyspark.sql.DataFrame
        A DataFrame consisting of the Texas Franchise taxholder data

    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame representing firm growth by year and city in Texas.

    Note
    ----
    The Texas Active Franchise Taxholder database allows us to understand
    exactly when businesses establish ground in Texas. We are able to
    manipulate this fact in a way to track business growth in Texas by
    observing the number of new franchise taxholders within a givin year.
    """
    return (
        df.filter(F.col("taxpayer_state") == "TX")
        .withColumn("responsibility_year", F.year("responsibility_date"))
        .groupBy("taxpayer_city", "responsibility_year")
        .agg(F.count("*").alias("firm_growth_count"))
        .orderBy(F.col("responsibility_year").desc)
    )
