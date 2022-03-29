import psycopg2
import os
import pandas as pd
import streamlit as st
from pyhive import presto
import cx_Oracle
import requests
import datetime
from io import StringIO,BytesIO
import boto3
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
import altair as alt
# import plotly.express as px
# import plotly.graph_objects as go
# from st_aggrid import AgGrid

conn =  presto.connect(host='*******',
                                    port=****,
                                    username='*****')
c = conn.cursor()

con = cx_Oracle.connect("*****","****","*****")
cursor = con.cursor()
cursor.execute("***************")

@st.cache(allow_output_mutation=True)
def add_data(sku,max):
    print(type(sku))
    print(type(max))
    query = "INSERT INTO sandbox.streamlit (SKU, MAX) VALUES ('%s', '%s')" % (sku, max)
    c.execute(query) 
    c.fetchall()    

@st.cache(allow_output_mutation=True)
def add_streamset_data(client,project,query):
    query = "INSERT INTO sandbox.config (client_name, project_name, query) VALUES ('%s', '%s', '%s')" % (client,project,query)
    c.execute(query) 
    c.fetchall()    

@st.cache(allow_output_mutation=True)
def upload_presto(df,BUCKET,SCHEMA,TABLE_NAME):
	filename = "demand_forecast_" + str(datetime.datetime.now())
	destination_path="{schema}/{table_name}/".format(schema=SCHEMA, table_name=TABLE_NAME)
	file_buffer = BytesIO()
	df.to_parquet(file_buffer,allow_truncated_timestamps=True)
	s3 = boto3.resource(
	service_name='s3',
	aws_access_key_id="*******",
	aws_secret_access_key="*********",
	endpoint_url ="http://******************/")
	s3.Bucket(BUCKET).put_object(Key=destination_path+filename, Body=file_buffer.getvalue())

# @st.cache(allow_output_mutation=True)
# def client1_conn():
# 	con = cx_Oracle.connect("*******","********","***********")
# 	cursor = con.cursor()
# 	cursor.execute("*********************")
# 	return con

@st.cache(allow_output_mutation=True)
def client2_conn():
	con = cx_Oracle.connect("************","*************","*************")
	cursor = con.cursor()
	cursor.execute("********************")
	return con

@st.cache(allow_output_mutation=True)
def forecast_api(data,option):
	BUCKET = "sandbox"
	SCHEMA = "schema"
	TABLE_NAME = "table"
	API = 'api'
	horizon = 30
	payload = {'series': data['QTY'].tolist(), 'frequency': 1, 'horizon':horizon, 'k': 2}
	req = requests.post(API+'arima', json=payload).json()
	date_list = [data['date'].max() + datetime.timedelta(days=x) for x in range(horizon)]
	store = []
	def test_zip(date_list,req):
		for f, b in zip(date_list,req):
			store.append((f, b))
	test_zip(date_list,req)
	store_1 = pd.DataFrame(store).rename(columns={0:'date',1:'value'})
	store_1['client_name'] = option
	store_1 = store_1[['client_name','date','value']]
	upload_presto(store_1,BUCKET,SCHEMA,TABLE_NAME)

@st.cache(allow_output_mutation=True)
def trigger(option):
	print(option)
	if option == 'Client1 Demand Forecast':
		data = pd.read_sql("select sum(originalqty) qty, trunc(timestamp) date from wx_orderdtl group by trunc(timestamp) order by trunc(timestamp)", client1())
		forecast_api(data,option)
	elif option == 'Client2 Demand Forecast':
		data = pd.read_sql("select sum(originalqty) qty, trunc(timestamp) date from wx_orderdtl group by trunc(timestamp) order by trunc(timestamp)", client2())
		forecast_api(data,option)
	elif option == 'Client1 section1 Demand':
		data = pd.read_sql("select sum(originalqty) qty, trunc(timestamp) date from wx_orderdtl WHERE ALLOCCODE = 'section1' group by trunc(timestamp) order by trunc(timestamp)", client1())
		forecast_api(data,option)
	elif option == 'Client1 section2 Demand':
		data = pd.read_sql("select sum(originalqty) qty, trunc(timestamp) date from wx_orderdtl WHERE ALLOCCODE = 'section2' group by trunc(timestamp) order by trunc(timestamp)", client1())
		forecast_api(data,option)
	elif option == 'Client1 section3 Demand':
		data = pd.read_sql("select sum(originalqty) qty, trunc(timestamp) date from wx_orderdtl WHERE ALLOCCODE LIKE 'section3%' group by trunc(timestamp) order by trunc(timestamp)", client1())
		forecast_api(data,option)
	elif option == 'Client1 section4 Demand':
		data = pd.read_sql("select sum(originalqty) qty, trunc(timestamp) date from wx_orderdtl WHERE ALLOCCODE = 'section4' group by trunc(timestamp) order by trunc(timestamp)", client1())
		forecast_api(data,option)
	elif option == 'Client1 section5 Demand':
		data = pd.read_sql("select sum(originalqty) qty, trunc(timestamp) date from wx_orderdtl WHERE ALLOCCODE = 'section5' group by trunc(timestamp) order by trunc(timestamp)", client1())
		forecast_api(data,option)

# @st.cache(allow_output_mutation=True)
def view_configuration():
	data = pd.read_sql("select * from sandbox.config", conn)	
	return data

@st.cache(allow_output_mutation=True)
def view_forecast_name():
	data = pd.read_sql("select project_name from sandbox.config", conn)	
	return data

def view_forecast(option):
	# data = pd.read_sql("select date_format(date_trunc('day',date),'%Y-%m-%d') as date,value from sandbox.demand_forecast where client_name", client1)		
	sql= "select date_trunc('day',date) as date,value from sandbox.demand_forecast where client_name = %s"	
	a = option
	data = pd.read_sql(sql,conn,params=(a,))
	data['date'] = pd.to_datetime(data['date'], format="%Y/%m/%d")
	# fig, ax = plt.subplots(figsize = (20,8))
	# sns.lineplot(data=data, x="date", y="value", ax=ax)
	fig, ax = plt.subplots(figsize = (30,10))
	fig = (
	alt.Chart(data.reset_index(), title="Demand forecast")
	.mark_line()
	.encode(x="date:T", y="value:Q")
	.interactive()
	).properties(
    width=850,
    height=300
	)
	fig	

s = f"""
<style>
div.stButton > button:first-child
{{
    width : 200px;
    padding: 15px 0;
    text-align: center;
    margin: 20px 10px;
    border-radius: 25px;
    font-weight: bold;
    border: 2px solid #FF0000;
    background: transparent;
    color: black;
    cursor: pointer;  
    position: relative;
    overflow: hidden;
 }}
<style>
"""

def main():
	st.set_page_config(layout="wide")
	# st.markdown(html_temp.format('royalblue','white'),unsafe_allow_html=True)
	st.markdown(
    """
    <style type='text/css'>
        details {
            display: none;
        }
    </style>
	""",
		unsafe_allow_html=True,
	)
	menu = ["Home","Add Configuration","View Configuration","Trigger DAG","View Forecast"]
	# choice = st.sidebar.selectbox("Menu",menu)
	all_options = view_forecast_name()
	st.sidebar.title("Navigation")
	choice = st.sidebar.radio("Go to", menu)
	st.markdown(s, unsafe_allow_html=True)
	if choice == "Home":
		# st.image("******.jpg")	
		st.subheader("Forecast Model Details and Visualization")
		st.write('''
		Forecasting Model is an internal tool that uses historical data as inputs to make informed estimates that are predictive in determining
		the direction of future trends.
		Through a combination of historical evidence, data, and a little instinct – businesses analyse past and current supply to predict future demand.
		Understanding how to properly forecast your supply chain will improve your relationship with suppliers and ensure you’re booking the right amount
		of cargo to avoid unnecessary costs.
		Businesses utilize forecasting to determine how to allocate their budgets or plan for anticipated expenses for an upcoming period of time.
		Plus, most importantly, you will know how to plan for expected and unexpected disruptions.
		You can perform all the activities with the help of simple user interface in which you can provide the required metrics
		and see the results in the final graphs.''')
	elif choice == "Add Configuration":
		st.header("Add Streamset details")		
		enter_client = st.text_input("Enter Client")
		enter_project = st.text_input("Enter Project Name")
		enter_query = st.text_input("Enter query")		
		if st.button("Add details"):
			add_streamset_data(enter_client,enter_project,enter_query)
			st.success("Configuration has been added")
	elif choice == "View Configuration":
		st.header("Streamset data")
		st.table(view_configuration())
	elif choice == "Trigger DAG":
		st.header("Demand Forecasting Models")
		option = st.selectbox('Model to be triggered?',(all_options))
		st.write('You selected:', option)
		if st.button("Trigger"):
			trigger(option)
			st.success("DAG has been triggered")		
	elif choice == "View Forecast":
		st.header("Charts")
		option = st.selectbox('Select Forecast',(all_options))		
		view_forecast(option)

if __name__ == "__main__":
    main()
