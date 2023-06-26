import subprocess
import os
from cresco_etl_helpers import CrescoLambdaClient
from postgres import Postgres
import json
import boto3
import botocore
import pandas as pd
import snow
import mssqlserver
import email_util
from io import BytesIO
from datetime import datetime, timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

current_dt = datetime.now().date()
start_dt = (current_dt - timedelta(30)).strftime('%Y-%m-%d')
havi_lambda_client = HAVILambdaClient()

snowflake = snow.Snowflake(WAREHOUSE='COMPUTE_WH', DATABASE='DEMO_DB', SCHEMA='ETL', ROLE='ETL_LOAD_ROLE')
sql_server_conn = json.loads(os.environ['SQL_SERVER'])
sql_server = mssqlserver.MSSqlServer(host=sql_server_conn['host'], user=sql_server_conn['user'],
                                     password=sql_server_conn['password'], port=sql_server_conn['port'],
                                     database=sql_server_conn['database'], driver=sql_server_conn['driver'])


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
                bucket_name = 'data-eng-cresco-labs'
                key = 'keys/' + file
                try:
                    local_file_name = '/tmp/' + file
                    s3.Bucket(bucket_name).download_file(key, local_file_name)
                    command = 'chmod 600 ' + local_file_name + ';'
                    subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
                except botocore.exceptions.ClientError as e:
                    raise RuntimeError(e)
        df_list = []
        for database in databases:
            connection_details = os.environ[database]
            connection_details_dict = json.loads(connection_details)
            sql = """SELECT i.id AS inventory_id,Btrim(i.strain) AS inventory_strain,
CASE
 WHEN p.name IS NULL THEN Btrim(i.strain)
 ELSE Btrim(p.name)
END As product_name,
pc.name as product_category,
p.strain AS bt_product_strain,
Date(i.created) as created,
CASE
    WHEN (i.inventorytype = -1) THEN 'Accessories'
    ELSE it.name
END AS inventory_type,
l.name AS location_name,
to_char(to_timestamp(s.sessiontime),'YYYY-MM-DD') AS date_tested,
i.sample_id AS sample_id,
 CASE WHEN s.result = 1 THEN 'Passed' WHEN s.result = -1 THEN 'Failed' ELSE 'Pending' END AS sample_status,cbd AS 
 bt_potency_cbd, thca AS bt_potency_thca, thc AS bt_potency_thc, cbdA AS bt_potency_cbda,qa_total AS bt_potency_total, 
 custom_1 AS Terpenes FROM inventory i JOIN (SELECT x.inventoryparentid,(qa_data-> 'CBD')::numeric AS cbd,
 (qa_data-> 'CBDA')::numeric AS cbda,(qa_data-> 'THC')::numeric AS thc,
 (qa_data-> 'THCA')::numeric AS thca,qa_data-> 'Total' AS qa_total,qa_data,y.sessiontime,y.inventoryid,
 y.result FROM (SELECT sa.inventoryparentid,sa.id,string_agg(CONCAT_WS('=>',r.name,r.value),',')::hstore as qa_data 
 FROM (SELECT MAX(id) AS id,inventoryparentid FROM bmsi_labresults_samples WHERE deleted = 0 GROUP BY 
 inventoryparentid) sa JOIN bmsi_labresults_potency_analysis r ON r.sample_id = sa.id GROUP BY 
 sa.inventoryparentid,sa.id) x JOIN bmsi_labresults_samples y ON y.id = x.id) s ON 
 s.inventoryparentid = i.inventoryparentid LEFT JOIN products p ON i.productid = p.id LEFT JOIN inventoryrooms ir 
 ON ir.id = i.currentroom INNER JOIN locations l ON l.id = i.location LEFT JOIN inventorytypes it 
 ON it.id = i.inventorytype LEFT JOIN productcategories pc ON p.productcategory = pc.id LEFT JOIN vendors v ON 
 p.defaultvendor = v.id  where i.created between '{start_date}' and '{current_date}' """\
                .format(start_date=start_dt, current_date=current_dt)

            postgres = Postgres(host=connection_details_dict['host'], user=connection_details_dict['user'],
                                password=connection_details_dict['password'], port=connection_details_dict['port'],
                                database=connection_details_dict['database'], ssl_mode='require',
                                ssl_cert='/tmp/postgresql.crt', ssl_key='/tmp/postgresql.key')
            product_inventory_df = postgres.read_sql_as_pandas_df(sql)
            if not product_inventory_df.empty:
                del product_inventory_df['terpenes']
                product_inventory_cat_req_df = product_inventory_df[["inventory_id", "product_name", "product_category",
                                                                 "inventory_type", "inventory_strain"]]
                product_list = product_inventory_cat_req_df.to_dict(orient='records')
                cat_df = pd.DataFrame(cresco_lambda_client.get_categorization(product_list))
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
                ['BRAND', 'BRAND_CODE', 'BT_INVENTORY_ID', 'BT_INVENTORY_STRAIN',
                 'CATEGORY', 'CBD_THC_RATIO', 'COUNT', 'CULTIVATOR', 'EXTRACTION_METHOD',
                 'FLAVOR', 'NAME', 'PRODUCT_CODE', 'PRODUCT_ID', 'PRODUCT_STRAIN_ID',
                 'STRAIN_CODE', 'STRAIN_TYPE', 'SUB_CATEGORY', 'SUB_CATEGORY_01',
                 'TERPENES', 'WEIGHT', 'WEIGHT_EACH', 'WEIGHT_UNIT',
                 'WEIGHT_VALUE', 'INVENTORY_ID', 'PRODUCT_NAME', 'PRODUCT_CATEGORY',
                 'INVENTORY_STRAIN', 'INVENTORY_TYPE', 'BT_POTENCY_CBD',
                 'BT_POTENCY_THCA', 'BT_POTENCY_THC', 'BT_POTENCY_CBDA',
                 'BT_POTENCY_TOTAL', 'LOAD_DT']]
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
