import os
import sys
import time
import asyncio
import aiohttp
from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from socrata.authorization import Authorization


async def _async_request(
    q: asyncio.Queue,
    session: aiohttp.ClientSession,
    result: List[Dict[str, Any]],
) -> None:
    """
    This function makes an asynchronous GET request using aiohttp and appends the result to the shared list.

    Parameters
    ----------
    q: asyncio.Queue
        A queue containing tuples of (url, params) which the function will use to send GET requests.
    session: aiohttp.ClientSession
        Client session which keeps track of cookies and headers and such for you over multiple requests.
    result: List[Dict[str, Any]]
        A shared list where the function stores the result of each GET request.
    """
    while not q.empty():
        url, params = await q.get()
        try:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        code=response.status,
                        headers=response.headers,
                        message="Failed to successfully pull up to {} attributes".format(
                            params["$offset"] + params["$limit"],
                        ),
                    )

                response_data = await response.json()

                print(
                    "Data successfully pulled for first {} attributes".format(
                        params["$offset"] + params["$limit"],
                    )
                )

                # Add the retrieved data to the list
                result.extend(response_data)
        except Exception as e:
            print(f"Error while processing: {str(e)}")


async def _asynchronous_get_request_to_json_endpoint(
    url: str,
    num_requests: int = 12,
) -> List[Dict[str, Any]]:
    """
    Send asynchronous GET requests to the JSON endpoint to retrieve all relevant
    up-to-date franchise tax holder information.

    Parameters
    ----------
    url: str
        The JSON endpoint for Franchise Tax Holder data.

    num_requests: int
        The number of simultaneous requests to be made.

    Returns
    ------
    List[Dict[str, Any]]
        A list of dictionaries representing Texas franchise tax holder information.
    """
    auth: Authorization = Authorization(
        domain=url,
        username=os.getenv("SOCRATA_USERNAME"),
        password=os.getenv("SOCRATA_PASSWORD"),
    )

    start_time: float = time.time()

    data_list: List[Dict[str, Any]] = []  # List to store the retrieved data
    q: asyncio.Queue = asyncio.Queue()  # Create a queue of requests

    params: Dict[str, int] = {"$limit": 50000, "$offset": 0}

    print("Retrieving Active Franchise Tax Permit Holder data...")
    print(f"Sending asynchronous GET request to {url} with {num_requests} requests at a time...")

    async with aiohttp.ClientSession() as session:
        while True:
            await q.put((url, params.copy()))  # Use copy to avoid reference issues

            # Check if there are more results
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    print(
                        "API Request successfully made with parameters $limit={} and $offset={}".format(
                            params["$limit"], params["$offset"]
                        )
                    )
                    response_data = await response.json()
                    if len(response_data) < params["$limit"]:
                        break
                else:
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        code=response.status,
                        headers=response.headers,
                        message="Failed to successfully pull up to {} attributes".format(
                            params["$offset"] + params["$limit"],
                        ),
                    )  # Stop creating new requests if the request failed

            params["$offset"] += params["$limit"]

        tasks = []
        for _ in range(num_requests):
            task = asyncio.create_task(_async_request(q, session, data_list))
            tasks.append(task)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

    end_time: float = time.time()
    execution_time: float = end_time - start_time
    dataset_size_megabytes: float = sys.getsizeof(data_list) / (1024**2)

    # Print the number of records retrieved, execution time, and memory
    print("Total Active Franchise Tax records retrieved:", len(data_list))
    print(f"Execution time: {execution_time:.1f} seconds")
    print(f"Dataset size: {dataset_size_megabytes:.3f} MegaBytes \n")

    # Example: Print the first record
    if data_list:
        first_record = data_list[0]
        print(f"First record: {first_record} \n")
    else:
        print("No records retrieved.")

    return data_list


def _write_list_to_spark_df(
    spark: SparkSession,
    data_list: List[Dict[str, Any]],
) -> DataFrame:
    """
    Write a list of dictionaries to a Spark DataFrame.

    Parameters
    ----------
    spark: SparkSession
        The SparkSession object.

    data_list: List[Dict[str, Any]]
        The input list of dictionaries where each dictionary represents a unique record.

    Returns
    -------
    pyspark.sql.DataFrame
        The spark DataFrame containing all scraped Active Franchise Taxholder data.
    """
    start_time: float = time.time()

    print("Transfering data from JSON structure to PySpark DataFrame...")
    df: DataFrame = spark.createDataFrame(data_list)

    end_time: float = time.time()
    execution_time: float = end_time - start_time

    print("Data successfully written to a PySpark DataFrame.")
    print("Showcasing schema...")
    df.printSchema()

    print(f"Execution time: {execution_time:.1f} seconds\n")

    return df


def retrieve_franchise_taxholder_df(
    spark: SparkSession,
    save_sample_to_csv: bool = False,
    output_file: str = "../../data/bronze/texas_comptrollers_office/franchise_tax_payment_sample.csv",
) -> DataFrame:
    """
    Retrieves franchise tax-holder data from the Texas Comptroller's
    Office database and converts the data into a PySpark DataFrame.

    The method employs multithreading to efficiently send GET requests to the
    Texas Comptroller's Office endpoint. The number of threads used for this operation
    is determined by the number of available CPU cores.

    The collected data can optionally be saved as a CSV file.

    Parameters
    ----------
    spark: SparkSession
        A SparkSession object to enable the creation of DataFrame.
    save_sample_to_csv: bool, optional
        If True, the first 1000 rows of the DataFrame are written to a CSV
        file. Default is False.
    output_file: str, optional
        The path of the output file where the DataFrame is to be written if
        save_sample_to_csv is set to True. Default is a specified path.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark DataFrame containing active franchise tax permit holder data.

    Raises
    ------
    IOError
        If the GET request fails to successfully pull the desired attributes.
    """

    # Retrieve necessary parameters to call GET request to franchise tax permit holder data
    FRANCHISE_TAX_API_ENDPOINT: str = "https://data.texas.gov/resource/9cir-efmm.json"

    # GET request to Texas Comptroller's Office endpoint
    json_data: List[Dict[str, Any]] = asyncio.run(
        _asynchronous_get_request_to_json_endpoint(FRANCHISE_TAX_API_ENDPOINT),
    )

    # Write JSON file to a Spark dataframe
    df: DataFrame = _write_list_to_spark_df(
        spark=spark,
        data_list=json_data,
    )

    # Write the DataFrame to a CSV file if prompted to do so
    if save_sample_to_csv:
        print(f"Writing Franchise Tax Data sample to CSV file to relative file path ({output_file})...")
        df.sample(withReplacement=False, fraction=500 / df.count(), seed=42).toPandas().to_csv(output_file)
        print(f"Sample successfully written to CSV file.\n")

    return df
