
import os
from dotenv import load_dotenv
import prestodb
import pandas as pd


# Read creadentials from local .env file in the same directory as this script
def get_credentials():

    load_dotenv()

    watsonx_data_creds = {}
    # Update the global variables that will be used for authentication in another function
    watsonx_data_creds["watsonx_data_host"] = os.getenv("WATSONX_DATA_HOST", None)
    watsonx_data_creds["watsonx_data_port"] = os.getenv("WATSONX_DATA_PORT", None)
    watsonx_data_creds["watsonx_data_username"] = os.getenv("WATSONX_DATA_USERNAME", None)
    watsonx_data_creds["watsonx_data_password"] = os.getenv("IBM_CLOUD_API_KEY", None)
    watsonx_data_creds["watsonx_data_catalog"] = os.getenv("WATSONX_DATA_CATALOG", None)
    watsonx_data_creds["watsonx_data_schema"] = os.getenv("WATSONX_DATA_SCHEMA", None)
    
    

    return watsonx_data_creds

def connect_to_data_source(watsonx_data_creds):

    #watsonx_data_creds = get_credentials()

    username=watsonx_data_creds["watsonx_data_username"]
    password=watsonx_data_creds["watsonx_data_password"]
    hostname=watsonx_data_creds["watsonx_data_host"]
    portnumber=watsonx_data_creds["watsonx_data_port"]
    print("username: ", username)
    print("hostname: ", hostname)
    print("port: ", portnumber)
    print("password: ", password)
    
    #catalog=watsonx_data_creds["watsonx_data_catalog"]
    #schema=watsonx_data_creds["watsonx_data_schema"]
    conn = prestodb.dbapi.connect(
        host=hostname,
        port=portnumber,
        user=username,
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication(username,password)
    )
    print(" in connect data source: ", conn)
    
    #conn = prestodb.dbapi.connect(
    #    host=hostname,
    #    port=portnumber,
    #    user=username,
    #    catalog=catalog,
    #    schema=schema,
    #    http_scheme='https',
    #    auth=prestodb.auth.BasicAuthentication(username,password)
    #)
    
    return conn

# Function to browse the data lakehouse and collect
# the catalogs / schemas / tables
def browse_lh(conn):
    all_catalogs = []
    all_schemas = []
    all_tables = []
    system_catalogs = ['jmx', 'system', 'tpcds', 'tpch']
    get_catalogs_sql_query = '''show catalogs'''
    print("connection: ", conn)
    df = pd.read_sql_query(get_catalogs_sql_query, conn)
    catalogs = df['Catalog'].tolist()
    for c in catalogs:
        if c in system_catalogs:
            continue
        all_catalogs.append(c)
        get_schemas_sql_query = f'''show schemas from {c}'''
        df_schema = pd.read_sql_query(get_schemas_sql_query, conn)
        schemas = df_schema['Schema'].tolist()
        all_schemas.append(schemas)
        schemaTables = []
        for s in schemas:
            get_tables_sql_query = f'''show tables from {c}.{s}'''
            df_tables = pd.read_sql_query(get_tables_sql_query, conn)
            tables = df_tables['Table'].tolist()
            schemaTables.append(tables)
        all_tables.append(schemaTables)
    
    return all_catalogs, all_schemas, all_tables
        

def browse_lh_v0(conn):
    all_catalogs = []
    all_schemas = []
    all_tables = []
    cur = conn.cursor()
    catalogs = cur.execute('show catalogs')
    for c in catalogs:
        catalogName = c[0]
        all_catalogs.append(catalogName)
        sql_statement = f'show schemas from {catalogName}'
        schemas = cur.execute(sql_statement)
        schemaList = []
        schemaTables = []
        for s in schemas:
            schemaName = s[0]
            schemaList.append(schemaName)
            sql_s = f'show tables from {catalogName}.{schemaName}'
            tables = cur.execute(sql_s)
            tableList = []
            for t in tables:
                tableName = t[0]
                tableList.append(tableName)
            schemaTables.append(tableList)
            
        all_schemas.append(schemaList)
        all_tables.append(schemaTables)
    return all_catalogs, all_schemas, all_tables