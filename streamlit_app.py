import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import warnings

from connectData import *
from watsonx_text_to_sql import * 

warnings.filterwarnings("ignore")

if 'connectionData' not in st.session_state:
    st.session_state['connectionData'] = False
if 'db_connection' not in st.session_state:
    st.session_state['db_connection'] = None

if 'catalogList' not in st.session_state:
    st.session_state['catalogList'] = None
if 'schemaList' not in st.session_state:
    st.session_state['schemaList'] = None
if 'tableList' not in st.session_state:
    st.session_state['tableList'] = None

if 'catalog' not in st.session_state:
    st.session_state['catalog'] = None
if 'schema' not in st.session_state:
    st.session_state['schema'] = None
if 'table' not in st.session_state:
    st.session_state['table'] = None

if 'sql_query' not in st.session_state:
    st.session_state['sql_query'] = None

# Using "with" notation
lh_creds = {}
with st.sidebar:
    st.title("Lakehouse Credentials")
    lh_creds['watsonx_data_host'] = st.text_input("Host Name")
    lh_creds['watsonx_data_port'] = st.text_input("Port Number")
    lh_creds['watsonx_data_username'] = st.text_input("username")
    lh_creds['watsonx_data_password'] = st.text_input("password",type="password")
    if st.button("Connect"):
        connection = connect_to_data_source(lh_creds)
        if connection:
            st.session_state['db_connection'] = connection

print("conn: ", st.session_state['db_connection'])
if st.session_state['db_connection'] != None and st.session_state['connectionData']==False:
    #print("connection is : ", st.session_state['db_connection'])
    [catalogs, schemas, tables] = browse_lh(st.session_state['db_connection'])
    st.session_state['connectionData'] = True
    #print("catalogs: ", catalogs)
    #print("schemas: ", schemas)
    #print("tables: ", tables)
    st.session_state['catalogList'] = catalogs
    st.session_state['schemaList'] = schemas
    st.session_state['tableList'] = tables

if st.session_state['connectionData'] == True:
    columns = st.columns(3)
    catalogs = st.session_state['catalogList']
    schemas = st.session_state['schemaList']
    tables = st.session_state['tableList']

    with columns[0]:
        st.header("Catalogs")
        catalog = st.selectbox("Select Catalog",catalogs)
        st.session_state['catalog'] = catalog

    with columns[1]:
        st.header("Schemas")
        catalog_idx = catalogs.index(catalog)
        schemaList = schemas[catalog_idx]
        schema = st.selectbox("Select Schema",schemaList)
        st.session_state['schema'] = schema

    with columns[2]:
        st.header("Tables")
        schema_idx = schemaList.index(schema)
        tableList = tables[catalog_idx][schema_idx]
        table = st.selectbox("Select Table", tableList)
        st.session_state['table'] = table

    conn = st.session_state['db_connection']
    user_input = st.text_input("Natual Language Input")
    
    if st.button('Generate SQL'):
    #user_input = "show me list of customers likely to churn based on their status"
        if user_input and conn and st.session_state['catalog'] != None and st.session_state['schema'] != None and st.session_state['table'] != None:
            sql_query = text_to_sql(user_input,st.session_state['catalog'],st.session_state['schema'],st.session_state['table'])
            if sql_query:
                st.session_state['sql_query'] = sql_query
    if st.session_state['sql_query'] != None:
        user_sql = st.text_input("SQL Query",st.session_state['sql_query'])
        col1, col2, col3 = st.columns(3)
        with col1:
            xval = st.text_input("xval")
        with col2: 
            yval = st.text_input("yval")
        with col3:
            color = st.text_input("color")
        if st.button("Execute SQL"):
            df = exec_sql_query(conn,user_sql)
            fig = px.bar(df, x=xval, y=yval, color=color, title="Churn by Marital Status",color_continuous_scale="reds")
            st.plotly_chart(fig, use_container_width=True, theme=None)
else: 
    print("connection details not provided")



        #st.selectbox("Catalogs,catalogs")
    

#chart_data = pd.DataFrame(np.random.randn(20, 3), columns=["a", "b", "c"])
csv_filename = "./customer_churn_data/customer_training_data.csv"
#chart_data = pd.read_csv(csv_filename)
#print('chart columns: ', chart_data.columns)
#st.bar_chart(chart_data,x="STATUS",y="CHURN",)


#long_df = px.data.medals_long()

##fig = px.bar(chart_data, x="STATUS", y="count", color="medal", title="Long-Form Input")
#fig.show()

# Plot!

##from connectData import *

##conn = connect_to_data_source()


#sql_query = '''SELECT STATUS, CHURN, COUNT(*) AS COUNT
#FROM
#  "watsonxiceberg"."churn_data"."customer_churn_data"
#GROUP BY STATUS, CHURN
#'''
#df = pd.read_sql_query(sql_query, conn)
###

###fig = px.bar(df, x="STATUS", y="COUNT", color="CHURN", barmode="group", title="Churn by Marital Status",color_continuous_scale="spectral",text_auto=True)
###
####fig = px.bar(df, x="STATUS", y="COUNT", color="CHURN", barmode="group", title="Churn by Marital Status",color_continuous_scale=[['T', 'red'],['F', 'green']],text_auto=True)
###
###
###st.plotly_chart(fig, use_container_width=True, theme="streamlit")

#st.plotly_chart(fig, use_container_width=True, theme=None)

#type = 'heatmap',
#              colorscale = 'Viridis'

#print(df.head())