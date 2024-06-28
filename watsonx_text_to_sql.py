import pandas as pd
from dotenv import load_dotenv
import os
from ibm_watsonx_ai import APIClient

def get_watsonx_creds():
    load_dotenv()

    watsonx_ai_creds = {}
    # Update the global variables that will be used for authentication in another function
    watsonx_ai_creds["watsonx_ai_url"] = os.getenv("WATSONX_AI_URL", "https://us-south.ml.cloud.ibm.com")
    watsonx_ai_creds["watsonx_ai_apikey"] = os.getenv("IBM_CLOUD_API_KEY", None)
    watsonx_ai_creds["space_id"] = os.getenv("WATSONX_AI_SPACE_ID", None)
    watsonx_ai_creds["deployment_id"] = os.getenv("WATSONX_AI_DEPLOYMENT_ID", None)

    return watsonx_ai_creds

# Extract sql query from text
def extract_sql_query(text):
    gentext = text.replace('\n','')
    dd = gentext.split("```")
    for d in dd:
        if d.startswith('SELECT') or d.startswith('select'):
            return d
    return None
    
def text_to_sql(text,catalog,schema,table):
    watsonx_credentials = get_watsonx_creds()

    credentials = {
        "url": watsonx_credentials["watsonx_ai_url"],
        "apikey": watsonx_credentials["watsonx_ai_apikey"]
    }

    client = APIClient(credentials)
    client.set.default_space(watsonx_credentials["space_id"])
    deployment_id = watsonx_credentials["deployment_id"]

    prompt_vars = {
        "catalog": catalog,
        "schema":schema,
        "tablename":table,
        "user_input":text
    }
    generated_response = client.deployments.generate_text(deployment_id,params={"prompt_variables": prompt_vars})
    #print('gen text: ', generated_response)
    sql_query = extract_sql_query(generated_response)
    #print("sql query return: ", sql_query)

#    target_table = f'{catalog}.{schema}.{table}'
#    sql_query = f'''SELECT STATUS, CHURN, COUNT(*) AS COUNT
#    FROM
#    {target_table}
#    GROUP BY STATUS, CHURN
#    '''

    return sql_query


def text_to_sql_v0(text,catalog,schema,table):
    target_table = f'{catalog}.{schema}.{table}'
    sql_query = f'''SELECT STATUS, CHURN, COUNT(*) AS COUNT
    FROM
    {target_table}
    GROUP BY STATUS, CHURN
    '''
    return sql_query

def exec_sql_query(conn,sql_query):
    df = pd.read_sql_query(sql_query, conn)
    return df