import base64
import json

def lambda_handler(event, context):
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        try:
            data_dict = json.loads(payload)
            metadata = data_dict.get('metadata', {})
            actual_data = data_dict.get('data', {})
            
            # Identify the data type
            data_type = metadata.get('table')
            is_valid = False

            # Specific Schema Checks
            if data_type == 'users':
                # CDC/Batch logic: Must have user_id and email
                if isinstance(actual_data.get('user_id'), int) and 'email' in actual_data:
                    is_valid = True
            
            elif data_type == 'clickstream':
                # Clickstream logic: Must have event_id and url
                if 'event_id' in actual_data and 'url' in actual_data:
                    is_valid = True

            status = 'Ok' if is_valid else 'ProcessingFailed'
                
        except Exception:
            status = 'ProcessingFailed'

        output.append({
            'recordId': record['recordId'],
            'result': status,
            'data': record['data']
        })
    return {'records': output}