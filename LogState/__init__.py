import json
import logging
import uuid

import azure.functions as func


def main(event: func.EventGridEvent, message: func.Out[str], context: func.Context):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    event_data = json.loads(event.get_json())
    logging.info(event_data)

    rowKey = str(uuid.uuid4())

    data = {
        "PartitionKey": event_data['businessDate'],
        "RowKey": event_data['cfgName'],
        "EmittingFunction": event_data['emittingFunction'],
        "LoggingFunction": context.function_name,
        "Process": event_data['process'],
        "State": event_data['state']
    }

    message.set(json.dumps(data))

    logging.info(f"Message created with the rowKey: {rowKey}")
