from collections import Counter
from datetime import date
from time import mktime
from airflow.decorators import dag, task
from airflow.operators.sqlite_operator import SqliteOperator
from datetime import datetime
import requests
import json
@dag(
    schedule=None,
    start_date=datetime(2023, 1, 11),
    catchup=False
)

def main_goal():

    def to_seconds_since_epoch(input_date: str) -> int:
        return int(mktime(date.fromisoformat(input_date).timetuple()))

    @task
    def readData() -> str:
        BASE_URL = "https://opensky-network.org/api"

        params = {
            "airport": "LFPG", # ICAO code for CDG
            "begin": to_seconds_since_epoch("2022-12-01"),
            "end": to_seconds_since_epoch("2022-12-02")
        }

        cdg_flights = f"{BASE_URL}/flights/departure"

        response = requests.get(cdg_flights, params=params).text

        return response
    
    @task
    def toJson(text: str):
        flights = json.loads(text)
        return flights
        

    response = readData()
    convert = toJson(response)

    response >> convert
    
_ = main_goal()

    
