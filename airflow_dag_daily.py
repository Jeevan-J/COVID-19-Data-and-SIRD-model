from __future__ import print_function

#from config import RAPID_API_HOST, RAPID_API_KEY
import time
from datetime import date, datetime, timedelta
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import mysql.connector as connector
import requests, json, pytz
from config import RAPID_API_HOST, RAPID_API_KEY, HOST, USER, PASS, DATABASE

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

args = {
    'owner': 'Airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'end_date': datetime(2020, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='covid_19_update',
    default_args=args,
    schedule_interval='0 */3 * * *',#@daily
    description='A simple workflow that fetches data from Rapid API and stores the data in MySQL database at regular intervals',
    tags=['covid_19']
)

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
                    raise AirflowException("Rapid API returned error status : "+ json_data['statusMsg']+" on GET: "+data_dict[province]["url"])
            else:
                raise AirflowException("Rapid API returned response status : "+ response.status_code+" on GET: "+data_dict[province]["url"])
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
                        print("Rapid API returned error status : "+ json_data['statusMsg']+" on GET: "+data_dict[province]["url"]+rec[0])#raise AirflowException("Rapid API returned error status : "+ json['statusMsg']+" on GET: "+data_dict[province]["url"])
                else:
                    print("Rapid API returned response status : "+ str(response.status_code)+" on GET: "+data_dict[province]["url"]+rec[0])#raise AirflowException("Rapid API returned response status : "+ response.status_code+" on GET: "+data_dict[province]["url"])
            #conn.commit()
            cursor.close()
            conn.close()
        time.sleep(5)
    return data_dict

fetching_data = PythonOperator(
    task_id='fetch_data',
    provide_context=False,
    python_callable=fetch_data,
    dag=dag,
)

def push_rt_data(**context):
    data_dict  = context['task_instance'].xcom_pull(task_ids='fetch_data')
    tz = pytz.timezone('Asia/Kolkata')
    add_record_to_state_rt = ("REPLACE INTO state_wise_rt_data "
            "(state_code, active_cases, deaths, confirmed_cases, new_deaths, recovered_cases, new_confirmed_cases, new_recovered_cases, last_updated_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    add_record_to_district_rt = ("REPLACE INTO district_wise_rt_data "
            "(state_code, district_name, notes, active_cases, confirmed_cases, recovered_cases, new_confirmed_cases, deaths, last_updated_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    conn = connector.connect(host=HOST,user=USER,passwd=PASS,database=DATABASE)
    cursor = conn.cursor()
    for province in data_dict.keys():
        if province == 'india':
            for record in data_dict[province]['data']:
                if record != []:
                    try:
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
                        print(record_data)
                    except:
                        print("May be not all fields are present. ",record)
            print("Done pushing for "+province)
        elif province == 'state':
            for record in data_dict[province]['data']:
                if record != []:
                    try:
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
                        print(record_data)
                    except:
                        print("May be not all fields are present. ",record)
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
                                        dis_record['deceased'],
                                        datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                                        )
                            cursor.execute(add_record_to_district_rt, record_data)
                            print(record_data)
                        except:
                            print("May be not all fields are present. ",dis_record)
            print("Done pushing for "+province)
    conn.commit()
    cursor.close()
    conn.close()
    return "Successfully loaded realtime data into database"


pushing_rt_data = PythonOperator(
    task_id='pushing_data_freq',
    provide_context=True,
    python_callable=push_rt_data,
    dag=dag,
)

def push_daily_data(**context):
    data_dict  = context['task_instance'].xcom_pull(task_ids='fetch_data')
    tz = pytz.timezone('Asia/Kolkata')
    if datetime.now(tz).time() > datetime(2020,1,1,22,0,0,0).time():
        add_record_to_india_daily = ("INSERT INTO daily_totalcases_india "
                "(date_of_record, deaths, total_deaths, confirmed_cases, total_confirmed_cases, recovered_cases,  total_recovered_cases) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        add_record_to_state_daily = ("INSERT INTO state_wise_daily_data "
                "(state_code, active_cases, deaths, confirmed_cases, new_deaths, recovered_cases, new_confirmed_cases, new_recovered_cases, date) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        add_record_to_district_daily = ("INSERT INTO district_wise_daily_data "
                "(state_code, district_name, notes, active_cases, deaths, confirmed_cases, recovered_cases, new_confirmed_cases, date) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        conn = connector.connect(host=HOST,user=USER,passwd=PASS,database=DATABASE)
        cursor = conn.cursor()
        for province in data_dict.keys():
            if province == 'india':
                for record in data_dict[province]['data']:
                    if record != []:
                        try:
                            record_data = (
                                        datetime.now(tz).date(),
                                        record['newdeaths'],
                                        record['deaths'],
                                        record['newconfirmed'],
                                        record['confirmed'],
                                        record['newrecovered'],
                                        record['recovered'],
                                        )
                            cursor.execute(add_record_to_india_daily, record_data)
                        except:
                            print("May be not all fields are present. ",record)
                print("Done pushing for "+province)
            elif province == 'state':
                for record in data_dict[province]['data']:
                    if record != []:
                        try:
                            record_data = (record['code'],
                                        record['active'],
                                        record['deaths'],
                                        record['confirmed'],
                                        record['newdeaths'],
                                        record['recovered'],
                                        record['newconfirmed'],
                                        record['newrecovered'],
                                        datetime.now(tz).date(),
                                        )
                            cursor.execute(add_record_to_state_daily, record_data)
                        except:
                            print("May be not all fields are present. ",record)
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
                                            dis_record['deceased'],
                                            dis_record['confirmed'],
                                            dis_record['recovered'],
                                            dis_record['newconfirmed'],
                                            datetime.now(tz).date(),
                                            )
                                cursor.execute(add_record_to_district_daily, record_data)
                            except:
                                print("May be not all fields are present. ",dis_record)
                print("Done pushing for "+province)
        conn.commit()
        cursor.close()
        conn.close()
        return "Done pushing data for Daily tables!"
    else:
        return "Not the end of the day"

pushing_daily_data = PythonOperator(
    task_id='pushing_data_daily',
    provide_context=True,
    python_callable=push_daily_data,
    dag=dag,
)

fetching_data >> [pushing_daily_data,pushing_rt_data]