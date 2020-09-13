import mysql.connector as connector
import requests, json
from datetime import date, datetime, timedelta

from config import RAPID_API_HOST, RAPID_API_KEY, HOST, USER, PASS, DATABASE

url = "https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaAllHistoricalData"

headers = {
    'x-rapidapi-host': RAPID_API_HOST,
    'x-rapidapi-key': RAPID_API_KEY
    }

response = requests.request("GET", url, headers=headers)

data = json.loads(response.text)['records']

conn = connector.connect(host=HOST,user=USER,passwd=PASS,database=DATABASE)
cursor = conn.cursor()

add_record = ("INSERT INTO daily_totalcases_india "
             "(date_of_record, deaths, total_deaths, confirmed_cases, total_confirmed_cases, recovered_cases, total_recovered_cases) "
             "VALUES (%s, %s, %s, %s, %s, %s, %s)")

for record in data:
    record_data = (datetime.strptime(record['dateofrecord'],"%Y-%m-%d").date(),
                   record['cases']['dailydeceased'],
                   record['cases']['totaldeceased'],
                   record['cases']['dailyconfirmed'],
                   record['cases']['totalconfirmed'],
                   record['cases']['dailyrecovered'],
                   record['cases']['totalrecovered'],
                   )
    cursor.execute(add_record, record_data)
    
conn.commit()
cursor.close()
conn.close()
print("Done loading all historical data!")