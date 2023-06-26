import boto3
import json
import uuid


class AWSLambdaClient:
    def __init__(self, region='us-west-2'):
        self.client = boto3.client('lambda', region_name=region)

    def get_body(self, function_name, payload):
        result = self.client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload)
        )
        payload = json.loads(result['Payload'].read())
        body = json.loads(payload['body']) if payload.get('body', None) else None
        return body


class HAVILambdaClient(AWSLambdaClient):

    def get_location_by_id(self, location_id):
        location_by_id_payload = {
            "pathParameters": {
                "table": 'locations',
                "id": location_id,
            },
            "requestContext": {
                "internal": True,
            }
        }
        body = self.get_body('havi-lab-api-service-int-get', location_by_id_payload)
        location = None
        if body:
            data = body.get('data', None)
            if data:
                location = data[0]
        return location

    def get_next_location(self, location_type, process_type, etl_type):
        next_location_payload = {
            "pathParameters": {
                "table": 'locations',
                "operation": 'lock-location',
            },
            "queryStringParameters": {
                "location_type": location_type,
                "etl_type": etl_type,
                "process_type": process_type,
            },
            "requestContext": {
                "internal": True,
            },
        }
        body = self.get_body('havi-lab-api-service-int-post_put', next_location_payload)
        data = None
        if body:
            data = body.get('data', None)
        return data

    def update_location(self, location):
        update_location_payload = {
            "pathParameters": {
                "table": 'locations',
                "id": 46,
            },
            "body": json.dumps(location),
            "requestContext": {
                "internal": True,
                "httpMethod": "PUT",
            }
        }
        body = self.get_body('havi-lab-api-service-int-post_put', update_location_payload)
        data = None
        if body:
            data = body.get('data', None)
        return data

    def get_categorization(self, item_list):
        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
        chunks = list(chunks(item_list, 500))
        data = []
        for chunk in chunks:
            json_dict = {"product_strain": "true", "categorization_request": chunk}
            categorization_payload = {
                "pathParameters": {
                    "table": 'categorization',
                    "operation": 'get-category',
                },
                "body": json.dumps(json_dict),
                "requestContext": {
                    "httpMethod": "POST",
                }
            }
            body = self.get_body('havi-lab-api-service-int-categorization', categorization_payload)
            if body:
                chunked_data = body.get('data', None)
                data.extend(chunked_data)
        return data

    def get_store_by_id(self, store_id):
        store_id_payload = {
            "pathParameters": {
                "table": 'stores',
                "id": store_id,
            },
            "requestContext": {
                "internal": True,
            },
        }
        body = self.get_body('havi-lab-api-service-int-get', store_id_payload)
        location = None
        if body:
            data = body.get('data', None)
            if data:
                location = data[0]
        return location

    def get_inventory_chunk(self, store_id, view, chunk_size, offset):
        inventory_chunk_payload = {
            "pathParameters": {
                "table": 'inventory',
            },
            "headers": {
                "store_id": store_id,
            },
            "queryStringParameters": {
                "view": view,
                "limit": chunk_size,
                "offset": offset,
            },
            "requestContext": {
                "internal": True,
            },
        }
        print(inventory_chunk_payload)
        body = self.get_body('havi-lab-api-service-int-get', inventory_chunk_payload)
        inventory_list = None
        total_rows = 0
        if body:
            data = body.get('data', None)
            total_rows = body.get('total_rows', 0)
            if data:
                inventory_list = data
        return inventory_list, total_rows

    def get_current_inventory(self, location, view):
        try:
            chunk_size = 500
            inventory_list, total_rows = self.get_inventory_chunk(location['store_id'], view, chunk_size, 0)
            if inventory_list and len(inventory_list) > 0 and total_rows > chunk_size:
                chunk_number = 1
                next_offset = 0
                while True:
                    next_offset = chunk_number * chunk_size
                    if next_offset >= total_rows:
                        break
                    next_chunk = self.get_inventory_chunk(location['store_id'], view, chunk_size, next_offset)
                    inventory_list.extend(next_chunk[0])
                    chunk_number += 1

            # TODO log etl event here
            return inventory_list
        except Exception as e:
            print("In exception")
            print(str(e))
            pass

    def create_etl_log(self, etl_log):
        etl_log_payload = {
            "pathParameters": {
                "table": 'etl_log',
            },
            "body": json.dumps(etl_log.__dict__),
            "requestContext": {
                "internal": True,
                "httpMethod": 'POST',
            }
        }
        try:
            body = self.get_body('havi-lab-api-service-int-post_put', etl_log_payload)
            etl_log_list = list()
            if body and body.get('data', None) and isinstance(body.get('data', None), list) \
                    and len(body.get('data', None)) > 0:
                for data in body['data']:
                    etl_log_list.append(data)
                return etl_log_list[0]
            else:
                raise Exception('There was an error creating a etl log entry {etl_log}'.format(etl_log=etl_log))
        except Exception:
            raise Exception('Error calling lambda function')

    def update_inventory(self, http_method, inventory):
        inventory_chunk_payload = {
            "pathParameters": {
                "table": 'inventory',
            },
            "requestContext": {
                "internal": True,
                "httpMethod": http_method,
            },
            "body": json.dumps(inventory)
        }
        try:
            body = self.get_body('havi-lab-api-service-int-post_put', inventory_chunk_payload)
            if body:
                data = body.get('data', None)
                if data:
                    return data
                else:
                    return None
        except Exception:
            raise Exception('Error calling insert/update inventory lambda function')

    def delete_inventory(self, inventory_ids):
        inventory_payload = {
            "pathParameters": {
                "table": 'inventory',
                "operation": 'batch',
            },
            "requestContext": {
                "internal": True,
            },
            "body": json.dumps(inventory_ids)
        }
        try:
            body = self.get_body('havi-lab-api-service-int-delete', inventory_payload)
            if body:
                data = body.get('data', None)
                if data:
                    return data
                else:
                    return None
        except Exception:
            raise Exception('Error calling delete lambda function')


class ETLLog:
    def __init__(self, batch_id, location_id, extract_type, event=None, event_type=None, content=None):
        self.batch_id = batch_id
        self.location_id = location_id
        self.extract_type = extract_type
        self.event = event
        self.event_type = event_type
        self.content = content


class ETLLogClient:
    def __init__(self, log_category, location_id):
        self.log_category = log_category
        self.location_id = location_id
        self.uuid = uuid.uuid1().hex

    def log_event(self, event, row_count=None):
        etl_log = ETLLog(self.uuid, self.location_id, self.log_category)
        if event == ETLLogEventsConstants.ETL_STARTED:
            etl_log.event = 'ETL Started'
            etl_log.content = 'ETL Started for location id {location}'.format(location=self.location_id)
            etl_log.event_type = ETLLogTypesConstants.STARTED
        elif event == ETLLogEventsConstants.ETL_COMPLETE:
            etl_log.event = 'ETL Finished'
            etl_log.content = 'ETL Finished for location id {location}'.format(location=self.location_id)
            etl_log.event_type = ETLLogTypesConstants.FINISHED
        elif event == ETLLogEventsConstants.PROCESSING_STARTED:
            etl_log.content = 'Processing {category} started for ${location}.'.format(category=self.log_category,
                                                                                      location=self.location_id)
            etl_log.event = 'Processing started'
        elif event == ETLLogEventsConstants.UPDATED:
            etl_log.content = 'Updated {row_count} rows.'.format(row_count=row_count)
            etl_log.event = 'Updated {category} '.format(category=self.log_category)
        elif event == ETLLogEventsConstants.INSERTED:
            etl_log.content = 'Inserted {row_count} rows.'.format(row_count=row_count)
            etl_log.event = 'Inserted {category} '.format(category=self.log_category)
        elif event == ETLLogEventsConstants.DELETED:
            etl_log.content = 'Deleted {row_count} rows.'.format(row_count=row_count)
            etl_log.event = 'Deleted {category} '.format(category=self.log_category)
        else:
            print("Illegal Log Event Exception")
        return etl_log

    def log_summary(self, etl_log_summary):
        etl_log = ETLLog(self.uuid, self.location_id, self.log_category)
        etl_log.event = 'Summary'
        etl_log.event_type = 'summary'
        etl_log.extract_type = self.log_category
        etl_log.errors = etl_log_summary["error"] if etl_log_summary.get("error", None) else None
        etl_log.is_error = True if etl_log_summary.get("error", None) else False
        etl_log.content = self.get_general_summary(etl_log_summary)
        return etl_log

    def log_error(self, error):
        etl_log = ETLLog(self.uuid, self.location_id, self.log_category)
        etl_log.event = self.log_category+' Processing Error'
        etl_log.event_type = ETLLogTypesConstants.ERROR
        etl_log.extract_type = self.log_category
        etl_log.errors = {'error': json.dumps(error), 'hasErrors': True}
        etl_log.is_error = True
        etl_log.content = str(error)
        return etl_log

    def get_general_summary(self, etl_log_summary):
        return "Inserted: {inserted}, Updated: {updated}," \
           "Deleted: {deleted} {log_category} rows for {location_id}." \
               "".format(inserted=etl_log_summary["inserted"], updated=etl_log_summary["updated"],
                         deleted=etl_log_summary["deleted"], log_category=self.log_category, location_id=self.location_id)


class ETLLogEventsConstants:
    ETL_STARTED = 'etl-started'
    BT_COMPLETE = 'bt-complete'
    HAVI_COMPLETE = 'havi-complete'
    PROCESSING_STARTED = 'processing-started'
    UPDATED = 'updated'
    INSERTED = 'inserted'
    DELETED = 'deleted'
    ETL_COMPLETE = 'etl-complete'
    ETL_SUMMARY = 'etl-summary'


class ETLLogTypesConstants:
    STARTED = 'started',
    PROCESSING = 'processing',
    SUMMARY = 'summary',
    ERROR = 'error',
    FINISHED = 'finished'
    WIB_SYNC = 'wib-sync'
    ETL_COMPLETE = 'etl-complete'


class ETLLogSummary:
    def __init__(self, inserted=None, insert_errors=None, updated=None, updated_rec=None, update_errors=None,
                 deleted=None, delete_errors=None):
        self.inserted = inserted
        self.insert_errors = insert_errors
        self.updated = updated
        self.updated_rec = updated_rec
        self.update_errors = update_errors
        self.deleted = deleted
        self.delete_errors = delete_errors


class ErrorHandler:
    @staticmethod
    def check_body_for_errors(self, body):
        if body.get('error', None):
            raise Exception(body['error'])
        elif body.get('errors', None):
            raise Exception(body['errors'])
