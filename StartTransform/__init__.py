# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging

import azure.functions as func
import azure.durable_functions as df

import json
import shared_code as sc
import datetime
import os
import re

async def main(event: func.EventGridEvent, starter: str) -> None:

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

    storage_account_name = os.environ['P19_STORAGE_ACCOUNT_NAME']
    storage_account_key = os.environ['P19_STORAGE_ACCOUNT_ACCESS_KEY']
    config_container_name = os.environ['P19_STORAGE_CONTAINER_CONFIG']

    # read gen config
    gen_config_blob_name = "gen_cfg.yml"
    gen_cfg = sc.read_file_cfg(config_container_name, gen_config_blob_name, storage_account_name, storage_account_key)

    # read file config
    file_config_blob_name = event_data['cfgName'] + ".yml"
    file_cfg = sc.read_file_cfg(config_container_name, file_config_blob_name, storage_account_name, storage_account_key)

    # prepare event data
    process = file_cfg['process']
    if process['type'] == "transformByPipeline":
        parameter_object = process['parameters']['pipeline_parameters']
        parameter_object['businessDate'] = event_data['businessDate']
    
    all_cfg = {**gen_cfg, **file_cfg}
    logging.info(all_cfg)


    # start orchestration
    client = df.DurableOrchestrationClient(starter)
    orchestration_function = sc.get_orchestration_func_name(process['type'], all_cfg['tasks'])
    instance_id = await client.start_new(orchestration_function, None, all_cfg)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    links = client.create_http_management_payload(instance_id)
    for key, value in links.items():
        logging.info(f"{key} --> {value}")
