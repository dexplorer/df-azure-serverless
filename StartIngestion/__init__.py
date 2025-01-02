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

#async def main(myblob: func.InputStream, starter: str) -> func.HttpResponse:
async def main(event: func.EventGridEvent, starter: str) -> None:

    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    #event_data = json.loads(event.get_json())
    event_data = event.get_json()
    logging.info(event_data)
    blob_url = event_data['url']
    logging.info(blob_url)

    match = re.search("blob.core.windows.net\/", blob_url)
    if match:
        container_blob_name = blob_url[match.start() + 22:]
    
    storage_account_name = os.environ['P19_STORAGE_ACCOUNT_NAME']
    storage_account_key = os.environ['P19_STORAGE_ACCOUNT_ACCESS_KEY']
    config_container_name = os.environ['P19_STORAGE_CONTAINER_CONFIG']

    # read gen config
    gen_config_blob_name = "gen_cfg.yml"
    gen_cfg = sc.read_file_cfg(config_container_name, gen_config_blob_name, storage_account_name, storage_account_key)

    # read xref config
    xref_config_blob_name = "cfg_xref.yml"
    xref_cfg = sc.read_xref_cfg(container_blob_name, config_container_name, xref_config_blob_name, storage_account_name, storage_account_key)

    # read file config
    file_config_blob_name = xref_cfg['cfg']
    file_cfg = sc.read_file_cfg(config_container_name, file_config_blob_name, storage_account_name, storage_account_key)

    # prepare event data
    business_date_regex_pattern = file_cfg['inbound_file']['business_date_regex_pattern']
    match = re.match(business_date_regex_pattern, container_blob_name)

    if match:
        business_date = match.group(1)

    process = file_cfg['process']
    if process['type'] == "ingestByPipeline":
        #pipeline_name = process['parameters']['pipeline_name']
        parameter_object = process['parameters']['pipeline_parameters']
        parameter_object['businessDate'] = business_date
        parameter_object['blobFullName'] = container_blob_name
        parameter_object['container'] = os.environ['P19_STORAGE_CONTAINER_IN_DATA_SOURCE']
        match = re.search("\/", container_blob_name)
        if match:
            parameter_object['blobName'] = container_blob_name[match.start() + 1:]
    
    all_cfg = {**gen_cfg, **file_cfg}
    #all_cfg = sc.merge(sc.merge(gen_cfg, xref_cfg), file_cfg)

    logging.info(all_cfg)


    # start orchestration
    client = df.DurableOrchestrationClient(starter)
    orchestration_function = sc.get_orchestration_func_name(process['type'], all_cfg['tasks'])
    instance_id = await client.start_new(orchestration_function, None, all_cfg)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    #return client.create_check_status_response(req, instance_id)
    links = client.create_http_management_payload(instance_id)
    for key, value in links.items():
        logging.info(f"{key} --> {value}")
