# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as df

import shared_code as sc

def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info(f"Starting the function OrchestrationIngestion")

    all_cfg = context.get_input()
    logging.info(all_cfg)

    # all process
    all_results = []

    # required pre-process activities
    for idx, pp_step in enumerate(all_cfg['pre_ingestion_tasks']):
        pp_result = None
        if pp_step['run'] and pp_step['required']:
            activity_function = sc.get_activity_func_name(pp_step['type'], all_cfg['tasks'])
            pp_result = yield context.call_activity(activity_function, all_cfg)

        if pp_result:
            logging.info(f"Activity {activity_function} returns \n"
                         f"{pp_result}")
            all_results.append(pp_result)

    # process 
    process = all_cfg['process']
    activity_function = sc.get_activity_func_name(process['type'], all_cfg['tasks'])
    process_result = yield context.call_activity(activity_function, all_cfg)

    logging.info(f"Activity {activity_function} returns \n"
                 f"{process_result}")
    all_results.append(process_result)

    # optional activities
    optional_tasks = []
    for pp_step in all_cfg['pre_ingestion_tasks']:
        if pp_step['run'] and not pp_step['required']:
            activity_function = sc.get_activity_func_name(pp_step['type'], all_cfg['tasks'])
            optional_tasks.append(context.call_activity(activity_function, all_cfg))

    optional_results = yield context.task_all(optional_tasks)
    all_results.extend(optional_results)

    # required logging tasks
    for idx, pp_step in enumerate(all_cfg['logging_tasks']):
        pp_result = None
        if pp_step['run'] and pp_step['required']:
            activity_function = sc.get_activity_func_name(pp_step['type'], all_cfg['tasks'])
            pp_result = yield context.call_activity(activity_function, all_cfg)

        if pp_result:
            logging.info(f"Activity {activity_function} returns \n"
                         f"{pp_result}")
            all_results.append(pp_result)

    # return [result1, result4]
    return all_results


main = df.Orchestrator.create(orchestrator_function)
