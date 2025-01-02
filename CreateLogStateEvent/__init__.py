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
import re

def main(allCfg: dict, outputEvent: func.Out[func.EventGridOutputEvent], context: func.Context) -> str:

    logging.info(f"Starting the function {context.function_name}")
    
    all_cfg = allCfg
    logging.info(all_cfg)

    # match = re.search(".yml", all_cfg["cfg"])
    # if match:
    #     cfg_name = all_cfg["cfg"][:match.start()]

    cfg_name = all_cfg["leaf_cfg"]

    if all_cfg["pattern"] == "ingest":
        # multi line string using parentheses
        out_event_data = (
            f'{{'
            f'"emittingFunction": "{context.function_name}", '
            f'"process": "{all_cfg["process"]["parameters"]["pipeline_name"]}", '
            f'"businessDate": "{all_cfg["process"]["parameters"]["pipeline_parameters"]["businessDate"]}", '
            f'"blobFullName": "{all_cfg["process"]["parameters"]["pipeline_parameters"]["blobFullName"]}", '
            f'"cfgName": "{cfg_name}", '
            f'"state": "Success"'
            f'}}'
        )
    elif all_cfg["pattern"] == "transform":
        # multi line string using parentheses
        out_event_data = (
            f'{{'
            f'"emittingFunction": "{context.function_name}", '
            f'"process": "{all_cfg["process"]["parameters"]["pipeline_name"]}", '
            f'"businessDate": "{all_cfg["process"]["parameters"]["pipeline_parameters"]["businessDate"]}", '
            f'"cfgName": "{cfg_name}", '
            f'"state": "Success"'
            f'}}'
        )
    elif all_cfg["pattern"] == "extract":
        # multi line string using parentheses
        out_event_data = (
            f'{{'
            f'"emittingFunction": "{context.function_name}", '
            f'"process": "{all_cfg["process"]["parameters"]["pipeline_name"]}", '
            f'"businessDate": "{all_cfg["process"]["parameters"]["pipeline_parameters"]["businessDate"]}", '
            f'"cfgName": "{cfg_name}", '
            f'"state": "Success"'
            f'}}'
        )

    logging.info(out_event_data)

    # emit event
    outputEvent.set(
        func.EventGridOutputEvent(
            id="test-id",
            data=out_event_data,
            subject="State",
            event_type="LogState",
            event_time=datetime.datetime.utcnow(),
            data_version="1.0"))

    logging.info(f"Ending the function {context.function_name}")

    return f"Returning from function {context.function_name}!"
