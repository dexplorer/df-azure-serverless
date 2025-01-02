# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import azure.functions as func

import datetime
import shared_code as sc
import os


def main(task: dict, outputEvent: func.Out[func.EventGridOutputEvent], context: func.Context) -> str:

    logging.info(f"Starting the function {context.function_name}")
    
    business_date = task['business_date']
    logging.info(business_date)
    cfg_name = task['cfg_name']
    logging.info(cfg_name)

    # multi line string using parentheses
    out_event_data = (
        f'{{'
        f'"emittingFunction": "{context.function_name}", '
        f'"businessDate": "{business_date}", '
        f'"cfgName": "{cfg_name}" '
        f'}}'
    )
    logging.info(out_event_data)


    storage_account_name = os.environ['P19_STORAGE_ACCOUNT_NAME']
    storage_account_key = os.environ['P19_STORAGE_ACCOUNT_ACCESS_KEY']
    config_container_name = os.environ['P19_STORAGE_CONTAINER_CONFIG']

    # read file config
    file_config_blob_name = cfg_name + ".yml"
    file_cfg = sc.read_file_cfg(config_container_name, file_config_blob_name, storage_account_name, storage_account_key)

    task_type = file_cfg['pattern']

    if task_type == "ingest":
        event_type = "RunIngestTask"
    elif task_type == "extract":
        event_type = "RunExtractTask"
    elif task_type == "transform":
        event_type = "RunTransformTask"
        
    if event_type:
        # emit event
        outputEvent.set(
            func.EventGridOutputEvent(
                id="test-id",
                data=out_event_data,
                subject="Task",
                event_type=event_type,
                event_time=datetime.datetime.utcnow(),
                data_version="1.0"))

    logging.info(f"Ending the function {context.function_name}")

    return f"Returning from function {context.function_name}!"
