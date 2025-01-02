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

from datetime import timedelta
from typing import Dict

def orchestrator_function(context: df.DurableOrchestrationContext):
    monitoring_request: Dict[str, int, int] = context.get_input()
    business_date: str = monitoring_request["businessDate"]
    minutes_to_monitor: int = monitoring_request["minutesToMonitor"]
    minutes_to_sleep: int = monitoring_request["minutesToSleep"]

    # Expiration of the monitoring
    expiry_time = context.current_utc_datetime + timedelta(minutes=minutes_to_monitor)
    while context.current_utc_datetime < expiry_time:
        # Check if dependencies have completed
        ready_to_run_tasks = yield context.call_activity("CheckDependency", business_date)

        # If we have tasks ready to run, we create events to trigger them
        if ready_to_run_tasks:
            for task in ready_to_run_tasks:
                # Create run task event
                yield context.call_activity("CreateRunTaskEvent", task)
        else:

            # No task is ready to run
            status = f"Nothing to run, for now ..."
            context.set_custom_status(status)
        
        # Schedule a new "wake up" signal
        next_check = context.current_utc_datetime + timedelta(minutes=minutes_to_sleep)
        yield context.create_timer(next_check)

    return "Monitor completed!"

main = df.Orchestrator.create(orchestrator_function)
