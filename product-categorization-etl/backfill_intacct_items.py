import pandas as pd
from datetime import datetime
import requests
from xml.etree.ElementTree import Element,ElementTree, SubElement, tostring, fromstring
import logging
import snow
import uuid
from bs4 import BeautifulSoup
import lxml
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# env = os.environ['ENV']
# print("Environment is : "+env)
# api credentials
url = 'https://api.intacct.com/ia/xml/xmlgw.phtml'
company_id = 'Cresco'
sender_id = 'Cresco Labs'
sender_password = 'jpt7RTk5SDTXTf+mV+jQIxRxAnJ1W8Z'
user_id = 'APIwebuser'
user_password = '2yXw!C9dF8Z'
headers = {
    'Content-Type': 'application/xml'
}
snowflake = snow.Snowflake(WAREHOUSE='COMPUTE_WH', DATABASE='DATA_ENG', SCHEMA='INTACCT', ROLE='ETL_LOAD_ROLE')


gld_cols = ['RECORDNO', 'BATCH_DATE', 'BATCH_TITLE', 'SYMBOL', 'BATCH_NO', 'BOOKID', 'CHILDENTITY',
            'MODIFIED', 'REFERENCENO', 'ADJ', 'MODULEKEY', 'LINE_NO', 'ENTRY_DATE', 'TR_TYPE',
            'DOCUMENT', 'ACCOUNTNO', 'ACCOUNTTITLE', 'STATISTICAL', 'DEPARTMENTID', 'DEPARTMENTTITLE', 'LOCATIONID',
            'LOCATIONNAME', 'CURRENCY', 'BASECURR', 'DESCRIPTION', 'DEBITAMOUNT', 'CREDITAMOUNT', 'AMOUNT',
            'TRX_DEBITAMOUNT', 'TRX_CREDITAMOUNT', 'TRX_AMOUNT', 'CLEARED', 'CLRDATE', 'CUSTENTITY', 'VENDENTITY',
            'EMPENTITY', 'LOCENTITY', 'RECORDTYPE', 'RECORDID', 'DOCNUMBER', 'STATE', 'WHENCREATED', 'WHENDUE',
            'WHENPAID', 'WHENMODIFIED', 'PRDESCRIPTION', 'PRCLEARED', 'PRCLRDATE', 'FINANCIALENTITY', 'TOTALENTERED',
            'TOTALPAID', 'TOTALDUE', 'ENTRYDESCRIPTION', 'GLENTRYKEY', 'CREATEDBY', 'BATCH_STATE', 'ENTRY_STATE',
             'PROJECTID', 'PROJECTNAME','CUSTOMERID', 'CUSTOMERNAME', 'VENDORID', 'VENDORNAME','EMPLOYEEID',
            'EMPLOYEENAME', 'ITEMID', 'ITEMNAME', 'CLASSID', 'CLASSNAME', 'RECORD_URL']

object_col_map = {'gldetail': gld_cols}


def clean_columns_dict_for_snowflake(cols):
    clean_cols_dict = {col: col.replace(".", "_") if "." in col else col for col in cols}
    return clean_cols_dict


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


def create_xml_request_get_object_read_by_query(session_id,intacct_object):
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
    query.text = ""
    pagesize = SubElement(readbyquery, 'pagesize')
    pagesize.text = "1000"
    return tostring(request)


def create_xml_request_query_ordered_by_batch_date(session_id, columns,intacct_object, max_modified_dt,offset):
    request = create_xml_request_context(session_id)
    operation = request.find("operation")
    content = operation.find("content")
    function = content.find("function")
    query = SubElement(function, 'query')
    obj = SubElement(query, 'object')
    obj.text = intacct_object
    select = SubElement(query, 'select')
    for col in columns:
        field = SubElement(select, 'field')
        field.text = col
    filter = SubElement(query, 'filter')
    greaterthanequalto = SubElement(filter, 'greaterthanorequalto')
    field = SubElement(greaterthanequalto, 'field')
    field.text = 'BATCH_DATE'
    value = SubElement(greaterthanequalto, 'value')
    value.text = max_modified_dt
    orderby = SubElement(query, 'orderby')
    order = SubElement(orderby, 'order')
    order_field = SubElement(order, 'field')
    order_field.text = 'BATCH_DATE'
    ascending = SubElement(order, 'ascending')
    pagesize = SubElement(query, 'pagesize')
    pagesize.text = "2000"
    offset_obj = SubElement(query, 'offset')
    offset_obj.text = str(offset)
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


def parse_object_response(xml_tree, cols):
    cols = [x.lower() for x in cols]
    objects = xml_tree.find("operation").find("result").find("data").children
    rows = []
    res = []
    for node in objects:
        if node:
            res = [child.text if child.name in cols else None for child in node.children]
            rows.append({cols[i]: res[i] for i, _ in enumerate(cols)})
    df = pd.DataFrame(rows, columns=cols)
    return df


def write_df(objects_df,intacct_object,session_id,cols):
    clean_cols_dict = clean_columns_dict_for_snowflake(cols)
    objects_df = objects_df.rename(columns=clean_cols_dict)
    snowflake.write_pandas_df(objects_df, intacct_object, 'intacct', None)


def load_intacct_object_from_api(session_id,intacct_object):
    session_id = get_session_id()
    count = 2000
    request = create_xml_request_query_ordered_by_batch_date(session_id,gld_cols, intacct_object, '01/01/2019', count)
    response = requests.post(url, data=request, headers=headers)
    xml_tree = BeautifulSoup(response.content, "lxml")
    no_of_objects = int(xml_tree .find("operation").find("result").find("data").get('totalcount'))
    cols = object_col_map[intacct_object]

    df = parse_object_response(xml_tree, cols)
    df['batch_date'] = pd.to_datetime(df['batch_date'])
    # max_batch_dt = df['batch_date'].max()
    # max_batch_dt = f"{max_batch_dt.day}/{max_batch_dt.month}/{max_batch_dt.year}"
    write_df(df, intacct_object, session_id, cols)
    count += 2000
    while count < no_of_objects:
        print(count)
        try:
            request = create_xml_request_query_ordered_by_batch_date(session_id, gld_cols, intacct_object, '01/01/2019',
                                                                     count)
            response = requests.post(url, data=request, headers=headers)
            xml_tree = BeautifulSoup(response.content, "lxml")
            df = parse_object_response(xml_tree, cols)
            if not df.empty:
                df['batch_date'] = pd.to_datetime(df['batch_date'])
                # max_batch_dt = df['batch_date'].max()
                # max_batch_dt = f"{max_batch_dt.day}/{max_batch_dt.month}/{max_batch_dt.year}"
                write_df(df, intacct_object, session_id, cols)
                count += 2000
            else:
                print("dataframe was empty")
        except Exception as e:
            print("Exception occured" )
            print(e)


def load_intacct_objects_to_snowflake(intacct_object):
    # children = [el.tag for el in list(items[0])]
    # Create the pandas DataFrame
    session_id = get_session_id()
    load_intacct_object_from_api(session_id, intacct_object)


def lambda_handler(event, context):
    logging.info("Running incremental daily ETL")
    try:
        objects = ['gldetail']
        for intacct_object in objects:
            load_intacct_objects_to_snowflake(intacct_object)
    except Exception as e:
        print('ETL Failed with : ' + str(e))
        return {"Execution": "Failed"}
    return {"Execution": "Success"}


if __name__ == "__main__":
    """Main method for local testing of lambda function"""
    lambda_handler(None, None)
