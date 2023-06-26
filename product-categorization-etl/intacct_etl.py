import pandas as pd
from datetime import datetime,timedelta
import requests
import intacct_objects as io
from xml.etree.ElementTree import Element, SubElement, Comment, tostring, fromstring, ElementTree
from xml.sax.saxutils import unescape
import logging
import database.snow as snow
import uuid
import os
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# env = os.environ['ENV']
# print("Environment is : "+env)
# api credentials
url = 'https://api.xxxx.com/ia/xml/xmlgw.phtml'
company_id = os.environ.get('company_id')
sender_id = os.environ.get('sender_id')
sender_password = os.environ.get('sender_password')
user_id = os.environ.get('user_id')
user_password = os.environ.get('user_password')
headers = {
    'Content-Type': 'application/xml'
}
#Snowflake connection
snowflake = snow.Snowflake(WAREHOUSE='COMPUTE_WH', DATABASE='DATA_ENG', SCHEMA='INTACCT', ROLE='ETL_LOAD_ROLE')

#Intacct Objects and corresponding column mapping
object_col_map = {'GLDETAIL': io.gld_cols, 'ITEM': io.item_cols,'GLACCOUNT': io.glaccount_cols, 'CLASS':io.class_cols,
                  'CUSTOMER': io.customer_cols,'VENDOR':io.vendor_cols ,'CONTACT':io.contact_cols , 'LOCATION':io.location_cols,
                  'DEPARTMENT':io.department_cols,'SODOCUMENTENTRY':io.sodocumentary_cols,'SODOCUMENT':io.sodocument_cols}

#Intacct objects with modified date information
object_modified_dt_col_map = {'GLDETAIL': 'MODIFIED', 'ITEM': 'WHENMODIFIED', 'GLACCOUNT': 'WHENMODIFIED',
                              'CLASS': 'WHENMODIFIED','CUSTOMER': 'WHENMODIFIED', 'VENDOR': 'WHENMODIFIED',
                              'CONTACT': 'WHENMODIFIED','LOCATION': 'WHENMODIFIED','DEPARTMENT': 'WHENMODIFIED',
                              'SODOCUMENTENTRY': 'WHENMODIFIED','SODOCUMENT': 'WHENMODIFIED'}

#column name clean up 
def clean_columns_dict_for_snowflake(cols):
    clean_cols_dict = {col: col.replace(".", "_") if "." in col else col for col in cols}
    return clean_cols_dict

#column name clean up 
def clean_columns_list_for_snowflake(cols):
    clean_cols = [col.replace(".", "_") if "." in col else col for col in cols]
    return clean_cols


def create_xml_request_context(session_id=None):
    request = Element('request')
    control = SubElement(request, 'control')
    senderid = SubElement(control, 'senderid')
    senderid.text = sender_id
    password = SubElement(control, 'password')
    password.text = sender_password
    controlid = SubElement(control, 'controlid')
    controlid.text = str(datetime.now().timestamp())
    uniqueid = SubElement(control, 'uniqueid')
    uniqueid.text = "false"
    dtdversion = SubElement(control, 'dtdversion')
    dtdversion.text = '3.0'
    includewhitespace = SubElement(control, 'includewhitespace')
    includewhitespace.text = "false"
    operation = SubElement(request, 'operation')
    authentication = SubElement(operation, 'authentication')
    if session_id:
        sessionid = SubElement(authentication, 'sessionid')
        sessionid.text = session_id
    else:
        login = SubElement(authentication, 'login')
        userid = SubElement(login, 'userid')
        userid.text = user_id
        companyid = SubElement(login, 'companyid')
        companyid.text = company_id
        login_password = SubElement(login, 'password')
        login_password.text = user_password
    content = SubElement(operation, 'content')
    function = SubElement(content, 'function')
    function.set('controlid', uuid.uuid1().hex)
    return request


def create_xml_request_get_object_read_by_query(session_id,intacct_object, start_dt, has_query):
    request = create_xml_request_context(session_id)
    operation = request.find("operation")
    content = operation.find("content")
    function = content.find("function")
    readbyquery = SubElement(function, 'readByQuery')
    obj = SubElement(readbyquery, 'object')
    obj.text = intacct_object
    fields = SubElement(readbyquery, 'fields')
    fields.text = "*"
    query = SubElement(readbyquery, 'query')
    if has_query:
        query_text = unescape(f"{object_modified_dt_col_map[intacct_object]} &gt;= '{start_dt}'")
        query.text = query_text
    pagesize = SubElement(readbyquery, 'pagesize')
    pagesize.text = "1000"
    return tostring(request)


def create_xml_request_get_object_read_more(session_id,intacct_object):
    request = create_xml_request_context(session_id)
    operation = request.find("operation")
    content = operation.find("content")
    function = content.find("function")
    readmore = SubElement(function, 'readMore')
    obj = SubElement(readmore, 'object')
    obj.text = intacct_object
    return tostring(request)


def create_xml_request_get_session():
    request = create_xml_request_context()
    operation = request.find("operation")
    content = operation.find("content")
    function = content.find("function")
    SubElement(function, 'getAPISession')
    return tostring(request)


def get_session_id():
    headers = {
      'Content-Type': 'application/xml'
    }
    session_id_request = create_xml_request_get_session()
    session_id_response = requests.post(url, data=session_id_request, headers=headers)
    session_response_body_as_xml = fromstring(session_id_response.content)
    session_xml_tree = ElementTree(session_response_body_as_xml)
    session_id = session_xml_tree.find("./operation/result/data/api/sessionid").text
    return session_id


def create_update_set_string(column_list, temp_tbl, target_tbl):
    set_cols_string = [target_tbl + "." + col + "=" + temp_tbl + "." + col for col in column_list]
    return 'set ' + ','.join(set_cols_string)


def create_insert_values_string(column_list, temp_tbl, target_tbl):
    columns_string = ','.join(column_list)
    temp_columns_string = ','.join([temp_tbl + "." + col for col in column_list])
    insert_sql = f'insert ({columns_string}) values ({temp_columns_string})'
    return insert_sql


def parse_object_response(xml_tree, cols):
    objects = xml_tree.findall("./operation/result/data/*")
    # children = [el.tag for el in list(objects[0])]
    rows = []
    for node in objects:
        res = []
        for el in cols:
            if node is not None and node.find(el) is not None:
                res.append(node.find(el).text)
            else:
                res.append(None)
        rows.append({cols[i]: res[i] for i, _ in enumerate(cols)})
    df = pd.DataFrame(rows, columns=cols)
    return df


def write_df(objects_df,intacct_object,cols):
    clean_cols_dict = clean_columns_dict_for_snowflake(cols)
    objects_df = objects_df.rename(columns=clean_cols_dict)
    snowflake.write_pandas_df(objects_df, intacct_object, 'intacct', None)


def load_intacct_objects_to_snowflake(backfill,intacct_object):
    # children = [el.tag for el in list(items[0])]
    # Create the pandas DataFrame
    session_id = get_session_id()
    df_list = []
    if backfill:
        has_query = False
    else:
        has_query = True
    start_dt = datetime.today() - timedelta(days=30)
    start_dt = start_dt.strftime('%m/%d/%Y 12:00:00')
    request = create_xml_request_get_object_read_by_query(session_id, intacct_object, start_dt, has_query)
    response = requests.post(url, data=request, headers=headers)
    response_body_as_xml = fromstring(response.content)
    xml_tree = ElementTree(response_body_as_xml)
    objects = xml_tree.findall("./operation/result/data/*")
    # children = [el.tag for el in list(objects[0])]
    # print(children)
    cols = object_col_map[intacct_object]
    df = parse_object_response(xml_tree, cols)
    clean_cols = clean_columns_list_for_snowflake(object_col_map[intacct_object])
    df.columns = clean_cols
    df_list.append(df)
    # write_df(df, intacct_object, session_id, cols)
    no_of_objects = int(xml_tree.find("./operation/result/data").get('totalcount'))
    count = 0
    if no_of_objects > 1000:
        count = 1000

    while count != 0 and count < no_of_objects:
        request = create_xml_request_get_object_read_more(session_id, intacct_object)
        response = requests.post(url, data=request, headers=headers)
        response_body_as_xml = fromstring(response.content)
        xml_tree = ElementTree(response_body_as_xml)
        df = parse_object_response(xml_tree, cols)
        df.columns = clean_cols
        if not df.empty:
            # write_df(df, intacct_object, session_id, cols)
            df_list.append(df)
            count += 1000
        else:
            print("dataframe was empty")
    final_df = pd.concat(df_list)
    final_df = final_df.reset_index(drop=True)
    if backfill:
        snowflake.write_pandas_df(final_df, intacct_object, 'intacct')
    else:
        intacct_object_temp = intacct_object + '_TEMP'
        snowflake.write_pandas_df(final_df, intacct_object_temp, 'intacct', if_exists='replace')
        merge_set_columns_string = create_update_set_string(clean_cols, intacct_object_temp, intacct_object)
        insert_columns_string = create_insert_values_string(clean_cols, intacct_object_temp, intacct_object)
        upsert_sql = f"merge into {intacct_object} using {intacct_object_temp} on {intacct_object}.recordno = " \
                     f"{intacct_object_temp}.recordno when matched then update {merge_set_columns_string} when not " \
                     f"matched then {insert_columns_string}"
        snowflake.sql(upsert_sql)
        drop_temp_sql = f"drop table {intacct_object_temp}"
        snowflake.sql(drop_temp_sql)


def lambda_handler(event, context):
    logging.info("Running incremental daily ETL")
    try:
        backfill = event.get("backfill", False) if event else False
        objects = ['ITEM','GLACCOUNT','CLASS', 'CUSTOMER', 'VENDOR', 'CONTACT', 'LOCATION', 'DEPARTMENT']
        for intacct_object in objects:
            logger.info("Loading object " + intacct_object)
            load_intacct_objects_to_snowflake(backfill, intacct_object)
            logger.info("Successfully loaded " + intacct_object)
    except Exception as e:
        print('ETL Failed with : ' + str(e))
        return {"Execution": "Failed"}
    return {"Execution": "Success"}


if __name__ == "__main__":
    """Main method for local testing of lambda function"""
    event = {"backfill": True}
    # lambda_handler(event, None)
    lambda_handler(None, None)