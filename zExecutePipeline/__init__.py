import logging

import azure.functions as func

import json
import os
import shared_code as sc

# adf imports
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time

def main(event: func.EventGridEvent, context: func.Context):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    event_data = json.loads(event.get_json())
    logging.info(event_data['emittingFunction'])

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
    #pipeline_name = os.environ["ADF_PIPELINE"]
    #pipeline_name = 'demoADBNotebook'
    pipeline_name = event_data['pipelineName']
    #pipeline_parameters = {}
    pipeline_parameters = event_data['pipelineParameters']
    logging.info(f"Pipeline parameters: {pipeline_parameters}")
    
    # Create a pipeline run
    run_response = adf_client.pipelines.create_run(rg_name, df_name, pipeline_name, parameters=pipeline_parameters)

    # Monitor the pipeline run
    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
    logging.info("\n\tPipeline run status: {}".format(pipeline_run.status))

    #filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    #query_response = adf_client.activity_runs.query_by_pipeline_run(rg_name, df_name, pipeline_run.run_id, filter_params)
    #sc.print_activity_run_details(query_response.value[0])
