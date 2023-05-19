import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


def list_json_to_df(list_json_res):
      list_df=[]
      for res in list_json_res:
            list_df.append(pd.DataFrame.from_records(res))
      df=pd.concat(list_df)      
      return(df)

class WindAPI:
      """
      a class used to treat the data from API
      """

      def __init__(self,url:str):
          """
          Parameters
          ----------
          url : str
            The url of the API

          to complete the headers we get the token from .env
          """
          self.url = url
          self.headers = { "X-Auth-Token": os.environ["Token"], "Content-Type": "application/json"}

      def get(self,payload_dict: dict = None, ref_id = None):
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
          if payload_dict:
               df[list(payload_dict.keys())[0]]=list(payload_dict.values())[0]
          return df
 
      def multithread_get(self,list_payload: list)-> pd.DataFrame:
          """
          function to get data from API using multithread, put into list and convert to df

          Args:
                list_payload(list): list to filter query from API

          Returns:
                pd.DataFrame: data from API into df
          """
          threads= []
          result=[]
          with ThreadPoolExecutor(max_workers=20) as executor:
                  for payload in list_payload:                  
                        threads.append(executor.submit(self.get,payload))                  
                  for task in as_completed(threads):
                        result.append(task.result())
          df = list_json_to_df(result)
          return(df)

     
      def sequential_get(self,list_payload: list)->pd.DataFrame:
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
          return(df)


         

     