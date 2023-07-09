import requests
import os

from requests import Response
from typing import Dict
from pyspark.sql import DataFrame, SparkSession

import pandas as pd


def _get_request_to_json_endpoint(
    url: str = "https://data.texas.gov/resource/jrea-zgmq.json",
) -> Dict:
    """
    Send a GET request to the JSON endpoint to retrieve all 
    relevant up-to-date franchise tax holder information. 

    Parameters
    ----------
    url: str
        The JSON endpoint for Franchise Tax Holder data.
    
    Return
    ------
    json: Dict
        A dictionary representing Texas franchise tax holder 
        information in a .json file format.
    """
    response: Response = requests.get(url)
    return response.json()


def compile_franchise_tax_data_into_spark_dataframe(
    spark: SparkSession,
    save_to_csv: bool = False,
    save_path: str = "../../data/bronze/texas-comptrollers-office/franchise_tax_payments.csv",
) -> DataFrame:
    """
    Pull franchise tax-holder data from the Texas Comptroller's
    Office database and emplace the data into a PySpark DataFrame.

    Parameters
    ----------
    spark: SparkSession

    Return
    ------
    pyspark.sql.DataFrame
        PySpark DataFrame containing franchise tax data.
    """
    
    json_data: Dict = _get_request_to_json_endpoint()
    df: DataFrame = spark.createDataFrame(json_data)

    if save_to_csv:
        df.toPandas().to_csv(save_path)

    return df


def read_franchise_tax_data_from_csv(
    spark: SparkSession,
    file_path: str = "../../data/bronze/texas-comptrollers-office/franchise_tax_payments.csv",
) -> DataFrame:
    """
    """
    return 
