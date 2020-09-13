from __future__ import print_function
import time
from datetime import date, datetime, timedelta

import mysql.connector as connector
import requests, json, pytz
from config import RAPID_API_HOST, RAPID_API_KEY, HOST, USER, PASS, DATABASE

tz = pytz.timezone('Asia/Kolkata')

#india_data, state_data, district_data = [],[],[]

#india_url = "https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaTotalCounts"
#state_url = "https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaStateWiseData"
#district_url = "https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaDistrictWiseDataForState?statecode="

data_dict = {'india':{"data":[],"url":"https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaTotalCounts"},
             'state':{"data":[],"url":"https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaStateWiseData"},
             'district':{"data":[],"url":"https://covid-19-india-data-by-zt.p.rapidapi.com/GetIndiaDistrictWiseDataForState?statecode="}}

headers = {
    'x-rapidapi-host': RAPID_API_HOST,
    'x-rapidapi-key': RAPID_API_KEY
    }

def fetch_data():
    global data_dict
    for province in data_dict.keys():
        if province == 'india' or province == 'state':
            response = requests.request("GET", data_dict[province]["url"], headers=headers)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                if(json_data['statusMsg'] == 'OK'):
                    data_dict[province]["data"] = json_data['data']
                else:
                    print("Rapid API returned error status : "+ json_data['statusMsg']+" on GET: "+data_dict[province]["url"])#raise AirflowException("Rapid API returned error status : "+ json_data['statusMsg']+" on GET: "+data_dict[province]["url"])
            else:
                print("Rapid API returned response status : "+ str(response.status_code) +" on GET: "+data_dict[province]["url"])#raise AirflowException("Rapid API returned response status : "+ response.status_code+" on GET: "+data_dict[province]["url"])
        else:
            conn = connector.connect(host=HOST,user=USER,passwd=PASS,database=DATABASE)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM state_codes")
            for rec in cursor:
                response = requests.request("GET", data_dict[province]["url"]+rec[0], headers=headers)
                if response.status_code == 200:
                    json_data = json.loads(response.text)
                    if(json_data['statusMsg'] == 'OK'):
                        data_dict[province]["data"].append({rec[0]:json_data['data']})
                    else:
                        print("Rapid API returned error status : "+ json_data['statusMsg']+" on GET: "+data_dict[province]["url"]+rec[0])#raise AirflowException("Rapid API returned error status : "+ json_data['statusMsg']+" on GET: "+data_dict[province]["url"]+rec[0])
                else:
                    print("Rapid API returned response status : "+ str(response.status_code)+" on GET: "+data_dict[province]["url"]+rec[0])#raise AirflowException("Rapid API returned response status : "+ response.status_code+" on GET: "+data_dict[province]["url"]+rec[0])
            cursor.close()
            conn.close()
        print("Done for "+province)
        time.sleep(5)
    return "Successfully fetched data from RapidAPI"

def push_data():
    global data_dict
    add_record_to_state_rt = ("REPLACE INTO state_wise_rt_data "
            "(state_code, active_cases, deaths, confirmed_cases, new_deaths, recovered_cases, new_confirmed_cases, new_recovered_cases, last_updated_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    add_record_to_district_rt = ("INSERT INTO district_wise_rt_data "
            "(state_code, district_name, notes, active_cases, confirmed_cases, recovered_cases, new_confirmed_cases, deaths) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    conn = connector.connect(host=HOST,user=USER,passwd=PASS,database=DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM state_codes")
    india_states = []
    for rec in cursor:
        india_states.append(rec)
    for province in data_dict.keys():
        if province == 'india':
            for record in data_dict[province]['data']:
                record_data = ("IN",
                               record['active'],
                               record['deaths'],
                               record['confirmed'],
                               record['newdeaths'],
                               record['recovered'],
                               record['newconfirmed'],
                               record['newrecovered'],
                               record['lastupdatedtime'],
                            )
                cursor.execute(add_record_to_state_rt, record_data)
            print("Done pushing for "+province)
        elif province == 'state':
            for record in data_dict[province]['data']:
                record_data = (record['code'],
                               record['active'],
                               record['deaths'],
                               record['confirmed'],
                               record['newdeaths'],
                               record['recovered'],
                               record['newconfirmed'],
                               record['newrecovered'],
                               record['lastupdatedtime'],
                            )
                cursor.execute(add_record_to_state_rt, record_data)
            print("Done pushing for "+province)
        else:
            for record in data_dict[province]['data']:
                state_code = list(record.keys())[0]
                for dis_record in record[state_code]:
                    if dis_record != []:
                        try:
                            record_data = (state_code,
                                        dis_record['name']+'$'+state_code,
                                        dis_record['notes'],
                                        dis_record['active'],
                                        dis_record['confirmed'],
                                        dis_record['recovered'],
                                        dis_record['newconfirmed'],
                                        dis_record['deceased']
                                        )
                            cursor.execute(add_record_to_district_rt, record_data)
                        except:
                            print("May be not all fields are present. ",dis_record)
            print("Done pushing for "+province)
    conn.commit()
    cursor.close()
    conn.close()
    return "Pushed data to MySQL"

print(fetch_data())
print(push_data())