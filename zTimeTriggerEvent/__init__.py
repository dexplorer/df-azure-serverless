import datetime
import logging

import azure.functions as func


def main(mytimer: func.TimerRequest, outputEvent: func.Out[func.EventGridOutputEvent], context: func.Context) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # emit event
    event_data = f'{{"emittingFunction": "{context.function_name}", "pipelineName": "runAnalytics"}}'
    logging.info(event_data)

    outputEvent.set(
        func.EventGridOutputEvent(
            id="test-id",
            data=event_data,
            subject="Extraction",
            event_type="ExecutePipeline",
            event_time=datetime.datetime.utcnow(),
            data_version="1.0"))

    logging.info(f"Event is emitted successfully")
