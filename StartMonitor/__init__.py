from datetime import datetime
from datetime import timezone
#from datetime import timezone as tzone
from dateutil import tz

#from pytz import timezone
import logging

import azure.functions as func
import azure.durable_functions as df


async def main(mytimer: func.TimerRequest, starter: str) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # define date format
    fmt = '%Y%m%d'
    # define eastern timezone
    #eastern = timezone('US/Eastern')
    eastern = tz.gettz('US/Eastern')
    # naive datetime
    naive_dt = datetime.now()
    # localized datetime
    loc_dt = datetime.now(tz=eastern)

    business_date = loc_dt.strftime(fmt)
    logging.info(f"business date: {business_date}")

    # emit event
    input = {"businessDate": business_date, "minutesToMonitor": 5, "minutesToSleep": 5}
    logging.info(input)

    # start orchestration
    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new("Monitor", None, input)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    #return client.create_check_status_response(req, instance_id)
    links = client.create_http_management_payload(instance_id)
    for key, value in links.items():
        logging.info(f"{key} --> {value}")

