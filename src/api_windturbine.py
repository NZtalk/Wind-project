import requests
import os
from pymongo import MongoClient
import pandas as pd


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


def insert_to_mongo(df, db_name, collection_name):
    """
    function to insert dataframe to mongodb
    Args:
          df(pd.DataFrame): dataframe to be inserted to mongodb
          db_name(str): name of the database
          collection_name(str): name of the collection
    Returns:
          None
    """
    client = MongoClient()
    db = client[db_name]
    collection = db[collection_name]
    records = df.to_dict('records')
    collection.insert_many(records)
    print(f"{len(records)} records inserted to MongoDB")


if __name__ == '__main__':
    url = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windturbines"
    payload = {"windfarm_id": "1ec6d7a1-4c09-6060-8336-df2d2b033685"}

    # get data from API and store in dataframe
    api = WindAPI(url)
    df = api.get(payload)

    # insert data into MongoDB
    db_name = "wind_db"
    collection_name = "wind_turbines_collection"
    insert_to_mongo(df, db_name, collection_name)
