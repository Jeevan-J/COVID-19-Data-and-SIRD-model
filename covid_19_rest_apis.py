from config import HOST,DATABASE,PASS,USER
from flask import Flask
import mysql.connector as connector
import json
import pandas as pd

app = Flask(__name__)


conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
SQL_Query = pd.read_sql_query('''select * from state_codes''', conn)
states_dict = pd.DataFrame(SQL_Query)
conn.close()

@app.route('/covid19')
def hello():
    return "Welcome, Covid-19 REST APIs!"

@app.route('/covid19/india/',defaults={"type":"realtime"})
@app.route('/covid19/india/<type>')
def india(type):
    if type == "realtime":
        try:
            conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
            SQL_Query = pd.read_sql_query('''select * from state_wise_rt_data where state_code="IN"''', conn)
            df = pd.DataFrame(SQL_Query)
            conn.close()
            return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
        except:
            return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
    elif type == "historical":
        try:
            conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
            SQL_Query = pd.read_sql_query('''select * from daily_totalcases_india''', conn)
            df = pd.DataFrame(SQL_Query)
            conn.close()
            return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
        except:
            return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
    else:
        return {"status":"Error","data":[{"error":"There is no API like '/covid19/india/"+type+"'. Use 'realtime' or 'historical' as endpoints"}]}

@app.route('/covid19/india/statecodes')
def states_codes():
    return {"status":"OK","data":json.loads(states_dict.to_json(orient='records'))}
    
@app.route('/covid19/india/statewise/',defaults={"type":"realtime","statecode":"all"})
@app.route('/covid19/india/statewise/<type>/',defaults={"statecode":"all"})
@app.route('/covid19/india/statewise/<type>/<statecode>')
def state(type, statecode):
    if type == "realtime":
        if statecode == "all":
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query('''select * from state_wise_rt_data''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        elif statecode in list(states_dict['state_code']):
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query(f'''select * from state_wise_rt_data where state_code="{statecode}"''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        else:
            return {"status":"Error","data":[{"error":"Provided state code doesn't exist, please visit '/covid19/india/statecodes' to see available states"}]}
    elif type == "historical":
        if statecode == "all":
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query('''select * from state_wise_daily_data''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        elif statecode in states_dict['state_code']:
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query(f'''select * from state_wise_daily_data where state_code="{statecode}"''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        else:
            return {"status":"Error","data":[{"error":"Provided state code doesn't exist, please visit '/covid19/india/statecodes' to see available states"}]}
    else:
        return {"status":"Error","data":[{"error":"There is no API like '/covid19/india/"+type+"'. Use 'realtime' or 'historical' as endpoints"}]}


@app.route('/covid19/india/districtwise/',defaults={"type":"realtime","statecode":"all"})
@app.route('/covid19/india/districtwise/<type>/',defaults={"statecode":"all"})
@app.route('/covid19/india/districtwise/<type>/<statecode>')
def district(type, statecode):
    if type == "realtime":
        if statecode == "all":
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query('''select * from district_wise_rt_data''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        elif statecode in list(states_dict['state_code']):
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query(f'''select * from district_wise_rt_data where state_code="{statecode}"''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        else:
            return {"status":"Error","data":[{"error":"Provided state code doesn't exist, please visit '/covid19/india/statecodes' to see available states"}]}
    elif type == "historical":
        if statecode == "all":
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query('''select * from district_wise_daily_data''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        elif statecode in states_dict['state_code']:
            try:
                conn = connector.connect(host=HOST,database=DATABASE,user=USER,passwd=PASS)
                SQL_Query = pd.read_sql_query(f'''select * from district_wise_daily_data where state_code="{statecode}"''', conn)
                df = pd.DataFrame(SQL_Query)
                conn.close()
                return {"status":"OK","data":json.loads(df.to_json(orient='records'))}
            except:
                return {"status":"Error","data":[{"error":"Error while connecting or fetching data from MySQL DB"}]}
        else:
            return {"status":"Error","data":[{"error":"Provided state code doesn't exist, please visit '/covid19/india/statecodes' to see available states"}]}
    else:
        return {"status":"Error","data":[{"error":"There is no API like '/covid19/india/"+type+"'. Use 'realtime' or 'historical' as endpoints"}]}



if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True,port=8081)