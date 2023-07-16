import os
import time
from pyspark.sql import functions as F, DataFrame, SparkSession


def convert_franchise_tax_data_to_firm_growth_by_city_and_year(
    old_df: DataFrame,
    save_to_csv: bool = False,
    output_file: str = "../../data/silver/texas_comptrollers_office/firm_growth.csv",
) -> DataFrame:
    """
    Converted raw franchise taxholder data from the Texas Comptroller's
    Office database to firm growth per year within each city in Texas.

    Parameters
    ----------
    old_df: pyspark.sql.DataFrame
        A DataFrame consisting of the Texas Franchise taxholder data
    save_to_csv: bool, Optional
        Saves firm growth data to a CSV file. Default is False
    output_file: str, Optional
        File directory to save the new Spark DataFrame to

    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame representing firm growth by year and city in Texas.

    Note
    ----
    The Texas Active Franchise Taxholder database allows us to understand
    exactly when businesses establish ground in Texas. We are able to
    manipulate this fact in a way to track business growth in Texas by
    observing the number of new franchise taxholders within a given year.
    """

    start_time: float = time.time()
    print("Creating firm growth dataframe by both city and year...")

    df: DataFrame = (
        old_df.filter(F.col("taxpayer_state") == "TX")
        .withColumn("responsibility_year", F.year("responsibility_beginning_date"))
        .groupBy("taxpayer_city", "responsibility_year")
        .agg(F.count("*").alias("firm_growth_count"))
        .orderBy(F.desc("responsibility_year"))
    )

    print("Successfully inputted Spark computation functionality for firm growth to a new DataFrame.")
    print("Showcasing snapshot of firm growth data...")

    df.show(n=10, truncate=False)

    end_time: float = time.time()
    execution_time: float = end_time - start_time
    print(f"Execution time: {execution_time:.1f} seconds\n")

    if save_to_csv:
        save_start_time: float = time.time()
        print(f"Writing Firm Growth data to CSV file in relative file path ({output_file})...")

        df.toPandas().to_csv(output_file)

        size_in_mb: float = os.stat(output_file).st_size / (1024**2)
        save_end_time: float = time.time()
        save_execution_time: float = save_end_time - save_start_time

        print(f"Data successfully written to CSV file.")
        print(f"Execution time: {save_execution_time:.1f} seconds.\nFile size: {size_in_mb:.1f} MegaBytes\n")

    return df
