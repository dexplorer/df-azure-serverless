# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from typing import List
import os
from azure.data.tables import TableClient
from azure.core.exceptions import HttpResponseError


def main(businessDate: str) -> List:

    business_date = businessDate

    connection_string = os.getenv(
        "P19_STORAGE_ACCOUNT_CONNECTION_STRING")

    table_name = "messages"
    table_client_msg = create_table_client(connection_string, table_name)

    parameters = {
        u"pk": business_date
    }
    query_filter = u"PartitionKey eq @pk and State eq 'Success'"
    select_list = [u"PartitionKey", u"RowKey"]
    comp_tasks = query_table(table_client=table_client_msg, query_filter=query_filter,
                             parameters=parameters, select_list=select_list)

    table_name = "dependencies"
    table_client_dep = create_table_client(connection_string, table_name)

    query_filter = u"IsActive eq true"
    select_list = [u"PartitionKey", u"DependentCfgName"]

    dep_tasks = query_table(table_client=table_client_dep, query_filter=query_filter,
                            parameters=parameters, select_list=select_list)

    logging.info(f"completed tasks"
                 f"{comp_tasks}")
    logging.info(f"dep tasks"
                 f"{dep_tasks}")

    all_tasks = {}
    pending_tasks = []
    comp_cfg = ""
    for dep in dep_tasks:
        task_cfg = dep["PartitionKey"]
        is_task_completed = False
        for comp_task in comp_tasks:
            comp_cfg = comp_task["RowKey"]
            if task_cfg == comp_cfg:
                is_task_completed = True
                break

        if is_task_completed:
            all_tasks[task_cfg] = "completed"
            logging.info(
                f"completed -> task cfg - {task_cfg} comp cfg {comp_cfg}")
        else:
            pending_tasks.append(dep)

    for dep in pending_tasks:
        task_cfg = dep["PartitionKey"]
        dep_cfg = dep["DependentCfgName"]
        logging.info(f"task cfg - {task_cfg}")
        logging.info(f"dep cfg - {dep_cfg}")

        is_dep_completed = False
        for comp_task in comp_tasks:
            comp_cfg = comp_task["RowKey"]
            logging.info(f"comp cfg - {comp_cfg}")
            if dep_cfg == comp_cfg:
                is_dep_completed = True
                break

        if is_dep_completed and task_cfg not in all_tasks:
            all_tasks[task_cfg] = "ready"
            logging.info(
                f"ready -> task cfg - {task_cfg} dep cfg - {dep_cfg} comp cfg {comp_cfg}")
        elif is_dep_completed:
            continue
        else:
            all_tasks[task_cfg] = "pending"
            logging.info(
                f"pending -> task cfg - {task_cfg} dep cfg - {dep_cfg} comp cfg {comp_cfg}")

    logging.info(f"All tasks"
                 f"{all_tasks}")

    ready_to_run_tasks = []
    for cfg in all_tasks:
        if all_tasks[cfg] == "ready":
            task = {}
            task['business_date'] = business_date
            task['cfg_name'] = cfg
            ready_to_run_tasks.append(task)

    logging.info(f"Ready to run tasks"
                 f"{ready_to_run_tasks}")

    return ready_to_run_tasks


def create_table_client(connection_string, table_name):

    table_client = TableClient.from_connection_string(
        conn_str=connection_string, table_name=table_name
    )
    logging.info(f"Table name: {table_client.table_name}")

    logging.info(table_client)
    return table_client


def query_table(table_client, query_filter, parameters, select_list):

    try:
        queried_entities = table_client.query_entities(
            query_filter=query_filter, select=select_list, parameters=parameters
        )

        returned_entities = []
        for entity in queried_entities:
            returned_entities.append(entity)

        return returned_entities

    except HttpResponseError as e:
        logging.info(e.message)
