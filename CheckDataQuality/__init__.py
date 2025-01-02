# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging


def main(allCfg: dict) -> str:

    logging.info(f"Starting the function CheckDataQuality")
    
    all_cfg = allCfg
    logging.info(all_cfg)
    
    return f"Hello from function CheckDataQuality!"
