import sys
import os
import requests
from requests.exceptions import RequestException
import time
from threading import Thread
import multiprocessing
from queue import Queue
from socrata.authorization import Authorization
from requests import Response
from typing import Dict, List, Any
from pyspark.sql import DataFrame, SparkSession, Row


def _worker(
    q: Queue,
    result: List[Dict[str, Any]],
) -> None:
    """
    This worker function is intended to be run in a separate thread and
    performs GET requests to a JSON endpoint, parsing the results and
    storing them in a shared list.

    Parameters
    ----------
    q: Queue
        A queue containing tuples of (url, params) which the worker will
        use to send GET requests. The worker will continue processing
        items from the queue until it is empty.

    result: List[Dict[str, Any]]
        A shared list where the worker stores the result of each
        GET request. Each item in the list is a dictionary representing a record
        from the response. This list is shared among all worker threads and
        is used to accumulate the results.

    Note
    ----
    The worker function will continue processing items from the queue until
    the queue is empty. If an error occurs while processing an item, the
    worker will log the error and continue with the next item in the queue.
    The worker function does not return a value; all results are stored in the
    shared `result` list.

    The worker function assumes that the GET request will return a JSON response
    containing a list of records. If the response does not meet these expectations,
    the worker may fail with an error.
    """
    while not q.empty():
        url, params = q.get()
        try:
            response: Response = requests.get(url, params=params)
            if response.status_code != 200:
                raise RequestException(
                    "Failed to successfully pull up to {} attributes".format(params["$offset"] + params["$limit"])
                )

            response_data: List[Dict[str, Any]] = response.json()

            print("Data successfully pulled for first {} attributes".format(params["$offset"] + params["$limit"]))

            # Add the retrieved data to the list
            result.extend(response_data)
        except Exception as e:
            print(f"Error while processing: {str(e)}")

        q.task_done()


def _multithreaded_get_request_to_json_endpoint(
    url: str,
    num_threads: int = 8,
) -> List[Dict[str, Any]]:
    """
    Send a GET request to the JSON endpoint to retrieve all
    relevant up-to-date franchise tax holder information.

    Requirements
    ------------
    The following environment variables must be configured.
    For Mac users, store in `~/.zshrc` or `~/.bash_profile`:
        - `SOCRATA_USERNAME`
        - `SOCRATA_PASSWORD`

    Parameters
    ----------
    url: str
        The JSON endpoint for Franchise Tax Holder data.

    Return
    ------
    json: List[Dict[str, Any]]
        A dictionary representing Texas franchise tax holder
        information in a .json file format.
    """
    # Make an auth object
    auth: Authorization = Authorization(
        domain=url,
        username=os.getenv("SOCRATA_USERNAME"),
        password=os.getenv("SOCRATA_PASSWORD"),
    )

    start_time: float = time.time()

    data_list: List[Dict[str, Any]] = []  # List to store the retrieved data
    q: Queue = Queue()

    # Create a queue of requests
    params: Dict[str, int] = {"$limit": 50000, "$offset": 0}

    print("Retrieving Active Franchise Tax Permit Holder data...")
    print(f"Multithreading GET request to {url} across {num_threads} CPU cores...")

    while True:
        q.put((url, params.copy()))  # Use copy to avoid reference issues

        # Check if there are more results
        response = requests.get(url, params=params)
        if response.status_code == 200:
            print(
                "API Request successfully made with parameters $limit={} and $offset={}".format(
                    params["$limit"], params["$offset"]
                )
            )
            response_data: List[Dict[str, Any]] = response.json()
            if len(response_data) < params["$limit"]:
                break
        else:
            raise RequestException(
                "API Request FAILED with parameters $limit={} and $offset={}".format(
                    params["$limit"], params["$offset"]
                )
            )  # Stop creating new requests if the request failed

        params["$offset"] += params["$limit"]

    # Create and start the threads
    for _ in range(num_threads):
        t = Thread(target=_worker, args=(q, data_list))
        t.start()

    # Wait for all tasks to complete
    q.join()

    # Tracker variables
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

    print("Showcasing snapshot of some of the DataFrame columns...")
    df.select("taxpayer_number", "taxpayer_name", "taxpayer_city", "current_exempt_reason_code").show(n=5)
    print(f"Execution time: {execution_time:.1f} seconds\n")

    return df


def retrieve_franchise_taxholder_df(
    spark: SparkSession,
    save_sample_to_csv: bool = True,
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
    NUM_THREADS: int = multiprocessing.cpu_count()

    # GET request to Texas Comptroller's Office endpoint
    json_data: Dict[str, Any] = _multithreaded_get_request_to_json_endpoint(
        url=FRANCHISE_TAX_API_ENDPOINT,
        num_threads=NUM_THREADS,
    )

    # Write JSON file to a Spark dataframe
    df: DataFrame = _write_list_to_spark_df(
        spark=spark,
        data_list=json_data,
    )

    # Write the DataFrame to a CSV file if prompted to do so
    if save_sample_to_csv:
        print(f"Writing Franchise Tax Data sample to CSV file to relative file path ({output_file})...")
        df.limit(1000).toPandas().to_csv(output_file)
        print(f"Sample successfully written to CSV file.\n")

    return df
