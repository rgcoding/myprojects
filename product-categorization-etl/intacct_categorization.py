import pandas as pd
from datetime import datetime
import os
import json
import numpy as np
import logging
import snow
import uuid
from havi_etl_helpers import HAVILambdaClient
# from multiprocessing import Pool
logger = logging.getLogger()
logger.setLevel(logging.INFO)
env = os.environ['ENV']
print("Environment is : "+env)
havi_lambda_client = HAVILambdaClient(env=env)
snowflake = snow.Snowflake(WAREHOUSE='COMPUTE_WH', DATABASE='CRESCO_API_DB', SCHEMA='ITEM_MAIN', ROLE='ETL_LOAD_ROLE')


def get_category(df):
    categ_list = df.to_dict('records')
    json_dict = {"product_strain": True, "ignore_category_check": True, "source": "intacct",
                 "categorization_request": categ_list}
    response = havi_lambda_client.get_categorization(json_dict)
    return pd.DataFrame(response)


def get_uuid():
    guid = uuid.uuid1().hex
    return guid


# def parallelize_dataframe(df, func, n_cores=4):
#     df_split = np.array_split(df, 5000)
#     pool = Pool(n_cores)
#     df = pd.concat(pool.map(func, df_split))
#     pool.close()
#     pool.join()
#     return df.reset_index()


def categorize_products():
    try:
        logging.info('Categorization start')
        sql = "select * from havi_api_db.item_main.stage_flat where finalized is null"
        product_df = snowflake.read_sql_as_pandas_df(sql)
        product_df['name'] = product_df['name'].replace("'", "")
        del product_df['strain_type']
        logging.info('Size of data before categorization ' + str(len(product_df)))
        if not product_df.empty:
            product_cat_req_df = product_df[["flat_id", "name", "category","brand","state","cultivator"]]
            product_cat_req_df.columns = ['inventory_id', 'product_name', 'source_category', 'source_brand',
                                          'source_state', 'source_cultivator']
            del product_df['brand']
            del product_df['name']
            del product_df['cultivator']
            del product_df['category']
            # cat_df = parallelize_dataframe(product_cat_req_df,get_category)
            if len(product_df) <= 1000:
                cat_df = get_category(product_cat_req_df)
            else:
                df_split = np.array_split(product_cat_req_df, 100)
                cat_list = []
                for df in df_split:
                    cat_df = get_category(df)
                    cat_list.append(cat_df)
                cat_df = pd.concat(cat_list)

            print(len(cat_df))
            print(list(cat_df.columns))
            cat_df = cat_df.reset_index()
            product_category_df = pd.merge(product_df, cat_df, how='inner', left_on='flat_id',
                                           right_on='bt_inventory_id')
            product_category_df['final_id'] = [uuid.uuid4().hex for _ in range(len(product_category_df.index))]
            product_category_df.columns = map(str.upper, product_category_df.columns)
            product_category_df = product_category_df.rename(columns={"PRODUCT_STRAIN_ID": "STRAIN_ID",
                                                                      "COUNT": "WEIGHT_COUNT"})
            product_category_temp = product_category_df[['FINAL_ID', 'FLAT_ID']]
            snowflake.write_pandas_df(product_category_temp, 'prod_cat_temp', 'item_main', if_exists='replace')
            product_category_df['ID'] = product_category_df['FINAL_ID']
            product_category_df['STRAIN'] = product_category_df['STRAIN_NAME']
            current_time = datetime.now()
            timestamp_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
            product_category_df['UPDATE'] = timestamp_str
            product_category_df = product_category_df[["ID", "NAME", "CATEGORY", "CULTIVATOR",
                                                       "BRAND", "WEIGHT", "WEIGHT_UNIT", "WEIGHT_VALUE", "STRAIN",
                                                       "STRAIN_TYPE", "FLAVOR", "CBD_THC_RATIO", "CONSUMPTION_METHOD",
                                                       "EXTRACTION_METHOD","PRODUCT_ID","STRAIN_ID", "CREATED", "UPDATE",
                                                       "STATE","SKU_ID","SUB_CATEGORY","SUB_CATEGORY_01","PRODUCT_CODE",
                                                       "BRAND_CODE","STRAIN_CODE","TERPENES","EXPERTISE_LEVEL",
                                                       "CONFIDENCE_LEVEL","CERTAINTY","WEIGHT_COUNT"]]

            product_category_df["CREATED"] = product_category_df["CREATED"].fillna(method='ffill')
            product_category_df["CREATED"] = product_category_df["CREATED"].astype(str)
            product_category_df["TERPENES"] = product_category_df["TERPENES"].astype(str)
            print("writing data to snowflake table final")
            update_sql = "update stage_flat set stage_flat.finalized = to_timestamp(CURRENT_TIMESTAMP()), " \
                         "final_id = prod_cat_temp.final_id from prod_cat_temp where stage_flat.flat_id =" \
                         " prod_cat_temp.flat_id"
            snowflake.write_pandas_df(product_category_df, 'final', 'item_main')
            snowflake.sql(update_sql)
            snowflake.sql("drop table havi_api_db.item_main.prod_cat_temp")
        else:
            logging.info('There are no records to process')
    except Exception as e:
        logging.error('There was an exception processing the location' + str(e))


def lambda_handler(event, context):
    logging.info("Running incremental daily ETL")
    try:
        categorize_products()
    except Exception as e:
        print('ETL Failed with : ' + str(e))
        return {"Execution": "Failed"}
    return {"Execution": "Success"}


if __name__ == "__main__":
    """Main method for local testing of lambda function"""
    # lambda_handler(event, None)
    lambda_handler(None, None)
