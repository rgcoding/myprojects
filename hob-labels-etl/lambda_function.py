
#
# Program - Process HOB Label data and load into SQL Server for reporting
# Authon - Raviraj Gubbala
#
import subprocess
import os
from havi_etl_helpers import HAVILambdaClient
from postgres import Postgres
import json
import boto3
import botocore
import pandas as pd
from database import snow
from database import mssqlserver
from hobsql import hobsql
import email_util
from io import BytesIO
from datetime import datetime, timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

current_dt = datetime.now().date()
start_dt = (current_dt - timedelta(30)).strftime('%Y-%m-%d')
havi_lambda_client = HAVILambdaClient()

#SNOW Flake connection information
snowflake = snow.Snowflake(WAREHOUSE='COMPUTE_WH', DATABASE='DEMO_DB', SCHEMA='ETL', ROLE='ETL_LOAD_ROLE')
sql_server_conn = json.loads(os.environ['SQL_SERVER'])
sql_server = mssqlserver.MSSqlServer(host=sql_server_conn['host'], user=sql_server_conn['user'],
                                     password=sql_server_conn['password'], port=sql_server_conn['port'],
                                     database=sql_server_conn['database'], driver=sql_server_conn['driver'])

#Formatting columns to standard
def format_columns(val):
    known_value = get_known_label(val)
    if known_value:
        return known_value
    separator = '_'
    if '_' not in val and '-' in val:
        separator = '-'
    elif '_' not in val and '-' not in val:
        separator = ' '
    frags = val.split(separator)

    if frags and len(frags) > 1:
        frags = list(filter(lambda x: x != 'id', frags))
    for x in range(len(frags)):
        if 'of' not in frags[x]:
            frags[x] = frags[x][0].upper() + frags[x][1:]
    return ' '.join(frags)

#Look up for column mapping values
def get_known_label(argument):
    switcher = {
        'cbd_1_to_1': 'CBD 1:1',
        'cbd_2_to_1': 'CBD 2:1',
        'cbd_4_to_1': 'CBD 4:1',
        'cbn': 'CBN',
        'pull_n_snap': 'Pull n Snap',
        'rso': "RSO",
        'bho': 'BHO',
        'co2': "CO2",
        'llr': "LLR",
        'cbd': "CBD",
        'thc': 'THC',
        'on_special': 'On Sale',
        'medical': 'Medical',
        'recreational': 'Adult Use'
    }
    return switcher.get(argument, False)


def lambda_handler(event, context):
    databases = ['LINCOLN', 'KANKAKEE','JOLIET']
    try:
        sql_server.truncate_table('joliet_il')
        security_files = ['postgresql.crt', 'postgresql.key']
        for file in security_files:
            file_path = '/tmp/' + file
            if not os.path.exists(file_path):
                s3 = boto3.resource('s3')
                bucket_name = 'data-eng-havi-labs'
                key = 'keys/' + file
                try:
                    local_file_name = '/tmp/' + file
                    s3.Bucket(bucket_name).download_file(key, local_file_name)
                    command = 'chmod 600 ' + local_file_name + ';'
                    subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
                except botocore.exceptions.ClientError as e:
                    raise RuntimeError(e)
        df_list = []
        #Looping through the different databases
        for database in databases:
            connection_details = os.environ[database]
            connection_details_dict = json.loads(connection_details)
            sql = hobsql.sql_lab_results.format(start_date=start_dt, current_date=current_dt)

            postgres = Postgres(host=connection_details_dict['host'], user=connection_details_dict['user'],
                                password=connection_details_dict['password'], port=connection_details_dict['port'],
                                database=connection_details_dict['database'], ssl_mode='require',
                                ssl_cert='/tmp/postgresql.crt', ssl_key='/tmp/postgresql.key')
            product_inventory_df = postgres.read_sql_as_pandas_df(sql)
            if not product_inventory_df.empty:
                #Getting terpenese values
                del product_inventory_df['terpenes']
                product_inventory_cat_req_df = product_inventory_df[["inventory_id", "product_name", "product_category",
                                                                 "inventory_type", "inventory_strain"]]
                product_list = product_inventory_cat_req_df.to_dict(orient='records')
                cat_df = pd.DataFrame(havi_lambda_client.get_categorization(product_list))
                if cat_df.empty:
                    continue
                product_category_df = pd.merge(product_inventory_df, cat_df, how='inner', left_on='inventory_id',
                                               right_on='bt_inventory_id')
                product_category_df['load_dt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                product_category_df.columns = map(str.upper, product_category_df.columns)
                df_list.append(product_category_df)
        appended_product_category_df = pd.concat(df_list)
        if not appended_product_category_df.empty:
            buffer = BytesIO()
            #Transforming the column values
            appended_product_category_df['BT_INVENTORY_ID'] = appended_product_category_df['BT_INVENTORY_ID']. \
                apply(lambda x: int(x))
            appended_product_category_df['INVENTORY_ID'] = appended_product_category_df['INVENTORY_ID']. \
                apply(lambda x: int(x))
            appended_product_category_df['TERPENES'] = appended_product_category_df['TERPENES']. \
                apply(lambda x: ','.join(x) if x is not None else '')
            appended_product_category_df['TERPENES'] = appended_product_category_df['TERPENES'].astype(str)
            appended_product_category_df['CBD_THC_RATIO'] = appended_product_category_df['CBD_THC_RATIO']. \
                apply(lambda x: format_columns(x) if pd.notnull(x) else x)
            appended_product_category_df['SUB_CATEGORY'] = appended_product_category_df['SUB_CATEGORY']. \
                apply(lambda x: format_columns(x) if pd.notnull(x) else x)
            appended_product_category_df['SUB_CATEGORY_01'] = appended_product_category_df['SUB_CATEGORY_01']. \
                apply(lambda x: format_columns(x) if pd.notnull(x) else x)
            string_cols = ['BT_INVENTORY_STRAIN', 'CATEGORY', 'STRAIN_TYPE', 'INVENTORY_TYPE']
            appended_product_category_df = appended_product_category_df.drop(['CERTAINTY'], axis=1)
            appended_product_category_df['EXTRACTION_METHOD'] = appended_product_category_df[
                'EXTRACTION_METHOD'].str.upper()
            for col in string_cols:
                appended_product_category_df[col] = \
                    appended_product_category_df[col].apply(lambda x: x.title() if pd.notnull(x) else x)

            appended_product_category_df = appended_product_category_df[
                hobsql.col_prod_category]
            sql_server.write_pandas_df(appended_product_category_df, 'joliet_il', 'dbo', if_exists='append')
            writer = pd.ExcelWriter('temp.xlsx', engine='xlsxwriter')
            writer.book.filename = buffer
            appended_product_category_df.to_excel(writer, sheet_name='HOB Labels')
            workbook = writer.book
            worksheet = writer.sheets['HOB Labels']
            format1 = workbook.add_format({'num_format': '0'})
            worksheet.set_column(3, 3, 18, format1)
            worksheet.set_column(24, 24, 18, format1)
            writer.save()
            recipients = os.environ['recipients'].split(',')
            text = buffer.getvalue()
            email_sender = email_util.Email('HOB Labels', recipients
                                            , text, 'Excel report attached',
                                            'HOB_Labels' + '.xlsx')
            email_sender.send_email()

    except Exception as e:
        print('ETL Failed with : ' + str(e))
        return {"Execution": "Failed"}
    return {"Execution": "Success"}


if __name__ == "__main__":
    """Main method for local testing of lambda function"""
    lambda_handler(None, None)
