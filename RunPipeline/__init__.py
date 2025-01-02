# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging

import os
import shared_code as sc

# adf imports
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time

def main(allCfg: dict) -> str:

    logging.info(f"Starting the function RunPipeline")
    
    all_cfg = allCfg
    logging.info(all_cfg)

    # Acquire a credential object
    credential = sc.get_credential()

    # Acquire a secret client object
    secret_client = sc.get_secret_client()

    # Retrieve the IDs and secret to use with ClientSecretCredential
    subscription_id = secret_client.get_secret("azure-subscription-id")

    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = os.environ["ADF_RESOURCE_GROUP"]

    # The data factory name. It must be globally unique.
    df_name = os.environ["ADF"]

    # Specify your Active Directory client ID, client secret, and tenant ID
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Create a pipeline with the copy activity
    #pipeline_name = all_cfg['pipelineName']
    pipeline_name = all_cfg['process']['parameters']['pipeline_name']
    pipeline_parameters = {}
    #pipeline_parameters['businessDate'] = all_cfg['businessDate']
    #pipeline_parameters['parameterObject'] = all_cfg['parameterObject']
    pipeline_parameters['parameterObject'] = all_cfg['process']['parameters']['pipeline_parameters']
    logging.info(f"Pipeline parameters: {pipeline_parameters}")
    
    # Create a pipeline run
    run_response = adf_client.pipelines.create_run(rg_name, df_name, pipeline_name, parameters=pipeline_parameters)

    # Monitor the pipeline run
    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
    logging.info("\n\tPipeline run status: {}".format(pipeline_run.status))

    filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(rg_name, df_name, pipeline_run.run_id, filter_params)
    sc.print_activity_run_details(query_response.value[0])

    return f"Ending activity function RunPipeline!"
    