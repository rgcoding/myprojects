import snow
import postgres
import logging
import os
from datetime import datetime
import emp_upload_sql
logger = logging.getLogger()
logger.setLevel(logging.INFO)


#Secure Portal connection - replace with prod location & move to secrets manager

now = datetime.now()
timestamp = now.isoformat()

pg_secret_name = os.environ['PG_SECRET_NAME']
region_name = os.environ['REGION_NAME']
snow_warehouse = os.environ['SNOW_WAREHOUSE']
snow_database = os.environ['SNOW_DATABASE']
snow_schema=os.environ['SNOW_SCHEMA']
snow_role=os.environ['SNOW_ROLE']
snow_secret_name=os.environ['SNOW_SECRET_NAME']

pg_con = postgres.Postgres(secret_name=pg_secret_name, region_name=region_name)
snowflake = snow.Snowflake(WAREHOUSE=snow_warehouse, DATABASE=snow_database,SCHEMA=snow_schema, ROLE=snow_role, SECRET_NAME=snow_secret_name, REGION_NAME=region_name)
                 

def get_employee(schema):
    emp_data = snowflake.read_sql_as_pandas_df(emp_upload_sql.sql_get_employee)
    return emp_data

def truncate_employee(schema):
    snowflake.sql(emp_upload_sql.sql_del_employee)
    

def load_to_pg(tablename,schema):
    """Load to Postgres"""
    pg_sql = f"truncate table {tablename};"
    pg_con.update_sql_via_ssh_tunnel(pg_sql)
    print("data loaded in table")

def trans_rem_char(df, col):
    df[col].replace(to_replace='[^0-9]+', value='',inplace=True,regex=True)


def lambda_handler(event, context):
    try:
        schema = 'hr'
        table='users_emp_temp'
        emp_data1 = get_employee(schema)
        if not emp_data1.empty:
            logger.info("Employee data is available for update. Update in progress..")
            trans_rem_char(emp_data1, 'phone')
            load_to_pg(table, 'public')
            pg_con.write_pandas_df(emp_data1, table, 'public' )
            pg_con.update_sql_via_ssh_tunnel(emp_upload_sql.update_users_sql_1)
            pg_con.update_sql_via_ssh_tunnel(emp_upload_sql.update_users_sql_2)
            truncate_employee(schema)
        else:
            logger.info("Employee data is not available for update. No updates.")
    finally:
        pass

if __name__ == "__main__":
    """Main method for local testing of lambda function"""
    lambda_handler(None, None)