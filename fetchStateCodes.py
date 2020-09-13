import mysql.connector as connector
import requests, json
from config import RAPID_API_HOST, RAPID_API_KEY, HOST, USER, PASS, DATABASE

url = "https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaStateCodesAndNames"

headers = {
    'x-rapidapi-host': RAPID_API_HOST,
    'x-rapidapi-key': RAPID_API_KEY
    }

response = requests.request("GET", url, headers=headers)

state_codes = json.loads(response.text)['data']

conn = connector.connect(host=HOST,user=USER,passwd=PASS,database=DATABASE) 

cursor = conn.cursor()

add_state = ("INSERT INTO state_codes "
             "(state_code, state_name) "
             "VALUES (%s, %s)")

for state in state_codes:
    cursor.execute(add_state, (state['code'],state['name']))
    
conn.commit()
cursor.close()
conn.close()
print("Done loading State codes and name!")