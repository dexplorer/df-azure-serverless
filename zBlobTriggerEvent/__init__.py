import logging
import json

import azure.functions as func

import shared_code as sc

import datetime
import os
import re

def read_master_cfg(in_blob_name, master_config_container_name, master_config_blob_name, storage_account_name, storage_account_key):

    blob_string = sc.read_blob_as_string(master_config_container_name, master_config_blob_name, storage_account_name, storage_account_key)
    yaml_obj = sc.parse_yaml_string(blob_string)

    for fc in yaml_obj['master_cfg']:
        blob_regex_pattern = fc['blob_regex_pattern']
        match = re.fullmatch(blob_regex_pattern, in_blob_name)

        if match:
            logging.info(f"Master config lookup found a match\n"
                         f"{fc}")
            break

    logging.info(f"Master config lookup is done")

    return fc

def read_file_cfg(file_config_container_name, file_config_blob_name, storage_account_name, storage_account_key):

    blob_string = sc.read_blob_as_string(file_config_container_name, file_config_blob_name, storage_account_name, storage_account_key)
    yaml_obj = sc.parse_yaml_string(blob_string)

    fc = yaml_obj['file_cfg']

    logging.info(f"File config is read\n"
                 f"{fc}")
    return fc

def main(myblob: func.InputStream, outputEvent: func.Out[func.EventGridOutputEvent], context: func.Context):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    # read master config
    storage_account_name = os.environ['P19_STORAGE_ACCOUNT_NAME']
    storage_account_key = os.environ['P19_STORAGE_ACCOUNT_ACCESS_KEY']
    master_config_container_name = os.environ['P19_STORAGE_CONTAINER_CONFIG']
    master_config_blob_name = "master_cfg.yml"
    master_cfg = read_master_cfg(myblob.name, master_config_container_name, master_config_blob_name, storage_account_name, storage_account_key)

    # read file config
    file_config_container_name = os.environ['P19_STORAGE_CONTAINER_CONFIG']
    file_config_blob_name = master_cfg['cfg']
    file_cfg = read_file_cfg(file_config_container_name, file_config_blob_name, storage_account_name, storage_account_key)

    # prepare event data
    business_date_regex_pattern = file_cfg['business_date_regex_pattern']
    match = re.match(business_date_regex_pattern, myblob.name)

    if match:
        business_date = match.group(1)
        logging.info(f"Business date lookup found a match\n"
                        f"{business_date}")

    process = file_cfg['process']
    if process['type'] == "ingestByPipeline":
        pipeline_name = process['parameters']['pipeline_name']
        logging.info(f"Pipeline name: {pipeline_name}")

    pipeline_parameters = {}
    pipeline_parameters['businessDate'] = business_date

    parameter_object = process['parameters']['pipeline_parameters']
    parameter_object['businessDate'] = business_date
    parameter_object['container'] = os.environ['P19_STORAGE_CONTAINER_IN_DATA_SOURCE']
    match = re.search("\/", myblob.name)
    if match:
        parameter_object['blobName'] = myblob.name[match.start() + 1:]
    
    pipeline_parameters['parameterObject'] = json.dumps(parameter_object)
    logging.info(f"Pipeline parameters: {pipeline_parameters}")

    # multi line string using parentheses
    event_data = (
        f'{{'
        f'"emittingFunction": "{context.function_name}", '
        f'"blobFullName": "{myblob.name}", '
        f'"pipelineName": "{pipeline_name}", '
        f'"businessDate": "{business_date}", '
        f'"pipelineParameters": {json.dumps(pipeline_parameters)}'
        f'}}'
    )
    logging.info(event_data)

    # emit event
    outputEvent.set(
        func.EventGridOutputEvent(
            id="test-id",
            data=event_data,
            subject="Ingestion",
            event_type="ExecutePipeline",
            event_time=datetime.datetime.utcnow(),
            data_version="1.0"))

    logging.info(f"Event is emitted successfully")
