import re
import os
import requests
from socrata.authorization import Authorization
from socrata import Socrata
from requests import Response
from typing import Dict, List, Tuple, Any
from pyspark.sql import (
    DataFrame, 
    SparkSession,
    types as T,
    functions as F,
)


def _get_request_to_json_endpoint(
    url: str
) -> Dict[str, Any]:
    """
    Send a GET request to the JSON endpoint to retrieve all 
    relevant up-to-date franchise tax holder information. 

    Requirements
    ------------
    The following environment variables must be configured. 
    For Mac, store in `~/.zshrc`:
        - SOCRATA_USERNAME
        - SOCRATA_PASSWORD
        - ACTIVE_FRANCHISE_TAX_HOLDER_ID
        - ACTIVE_FRANCHISE_TAX_HOLDER_SECRET
    
    Parameters
    ----------
    url: str
        The JSON endpoint for Franchise Tax Holder data.
    
    Return
    ------
    json: Dict[str, Any]
        A dictionary representing Texas franchise tax holder 
        information in a .json file format.
    """

    # Boilerplate...
    # Make an auth object
    auth: Authorization = Authorization(
        domain = url,
        username = os.getenv("SOCRATA_USERNAME"),
        password = os.getenv("SOCRATA_PASSWORD"),
    )

    params: Dict[str, int] = {"$limit": 50000, "$offset": 0}  # Set the desired limit of results per request
    data_dict: Dict[str, Any] = {}  # Dictionary to store the retrieved data

    while True:
        response: Response = requests.get(url, params=params)

        if response.status_code == 200:
            print("Data successfuly pulled for first {} attributes".format(
                params['$offset'] + params['$limit']
            ))
        else:
            raise IOError("Failed to successfully pull up to {} attributes".format(
                params['$offset'] + params['$limit']
            ))

        response_data: Dict[str, Any] = response.json()

        print(response_data[0])
        
        # Add the retrieved data to the dictionary
        for record in response_data:
            # Assuming there is a unique identifier in the record, use it as the key
            record_id: str = record["taxpayer_number"]
            data_dict[record_id] = record
        
        # Check if there are more results
        if len(response_data) < params["$limit"]:
            break
        
        # Set the offset for the next request
        params["$offset"] = params.get("$offset", 0) + params["$limit"]

    
    # Print the number of records retrieved
    print("Total Active Franchise Tax records retrieved:", len(data_dict))

    # Access the data dictionary for further processing
    # Example: Print the first record
    if data_dict:
        first_record = next(iter(data_dict.values()))
        print("First record:", first_record)
    else:
        print("No records retrieved.")

    return data_dict


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
        PySpark DataFrame containing active franchise taxholder data
    """

    FRANCHISE_TAX_API_ENDPOINT: str = "https://data.texas.gov/resource/9cir-efmm.json"
    json_data: Dict[str, Any] = _get_request_to_json_endpoint(FRANCHISE_TAX_API_ENDPOINT)
    df: DataFrame = spark.createDataFrame(json_data)

    if save_to_csv:
        df.toPandas().to_csv(save_path)

    return df


def read_franchise_tax_data_from_csv(
    spark: SparkSession,
    file_path: str = "../../data/bronze/texas-comptrollers-office/franchise_tax_payments.csv",
) -> DataFrame:
    """
    Reads Active Franchise Tax holder data from .csv file to a Spark DataFrame.

    Parameters
    ----------
    spark: SparkSession

    Return
    ------
    file_path: str
        The file path of the Active Franchise Tax Permit Holders `.csv` file.
    """

    df: DataFrame = spark.read.csv(
        file_path,
        header = True,
        inferSchema = True
    )

    cleaned_columns: List[str] = [re.sub(r'\s+', '_', column.strip()).lower() for column in df.columns]
    return df.toDF(*cleaned_columns)
