import requests
import os
from pymongo import MongoClient
import pandas as pd
import time

class WindAPI:
    """
    a class used to treat the data from API
    """

    def __init__(self, url: str):
        """
        Parameters
        ----------
        url : str
            The url of the API
        to complete the headers we get the token from .env
        """
        self.url = url
        self.headers = {"X-Auth-Token": "1DA16786338EFD7C1395B458CD7F8EB2058E6125E415C87CC470C09E51B59F6F", "Content-Type": "application/json"}

    def get(self, payload_dict: dict = None):
        """
        function to get data from API, convert to json and then convert to df
        Args:
              payload_dict (dict, optional): dict to filter results from API
        Returns:
              pd.DataFrame: data from API into df
        """
        response = requests.request("GET", self.url, json=payload_dict, headers=self.headers)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        return df

    def multithread_get(self, list_payload: list) -> pd.DataFrame:
        """
        function to get data from API using multithread, put into list and convert to df
        Args:
              list_payload(list): list to filter query from API
        Returns:
              pd.DataFrame: data from API into df
        """
        threads = []
        result = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            for payload in list_payload:
                threads.append(executor.submit(self.get, payload))
            for task in as_completed(threads):
                result.append(task.result())
        df = list_json_to_df(result)
        return df

    def sequential_get(self, list_payload: list) -> pd.DataFrame:
        """
        function to get data from API with a loop over list_payload, put into list and convert to df
        Args:
              list_payload(list): dict to filter query from API
        Returns:
              pd.DataFrame: data from API into df
        """
        list_df = []
        for payload in list_payload:
            df = pd.DataFrame.from_records(self.get(payload))
            list_df.append(df)
        df = pd.concat(list_df)
        return df

def insert_to_mongo(df, collection):
    """
    Function to insert dataframe to mongodb

    Args:
        df (pd.DataFrame): dataframe to be inserted to mongodb
        collection (pymongo.collection.Collection): collection object from PyMongo

    Returns:
        None
    """
    records = df.to_dict('records')
    result = collection.insert_many(records)
    print(f"{len(result.inserted_ids)} records inserted to MongoDB")


if __name__ == '__main__':
    # Define the API URL and payload
    url = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-scada-data"
    payload = {
        "windturbine_id": "1ec6d7ce-9481-661c-97ac-53533bbf8f7e",
        "start_date": "2023-04-27 00:00:00",
        "end_date": "2023-05-09 19:22:00"
    }

    # Define the database and collection names
    db_name = "wind_db"
    collection_name = "wind_collection_scada23"

    # Connect to MongoDB and insert initial data
    client = MongoClient()
    db = client[db_name]
    collection = db[collection_name]
    df = WindAPI(url).get(payload)
    insert_to_mongo(df, collection)

    # Continuously update the database every 6 seconds with new data after the end date
    while True:
        # Check if it's time to start updating the database
        now = pd.to_datetime("now")
        end_date = pd.to_datetime(payload["end_date"])
        if now >= end_date:
            # Set start date to be 10 minutes after the current time
            start_date = now + pd.Timedelta(minutes=10)

            # Update the payload with the new start and end dates
            payload["start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S")
            payload["end_date"] = "now"

            # Get the new data and insert it into the database
            df = WindAPI(url).get(payload)
            insert_to_mongo(df, collection)

        # Set time to 10 min for new scala data
        time.sleep(600)
