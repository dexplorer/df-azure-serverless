import logging

# storage blob and yaml imports
import os
from azure.storage.blob import BlockBlobService
import yaml
from io import StringIO

# key vault imports
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# regex imports
import re

# storage blob and yaml
def read_blob_as_string(container_name, blob_name, storage_account_name, storage_account_key):
    storage_account_name = os.environ['P19_STORAGE_ACCOUNT_NAME']
    storage_account_key = os.environ['P19_STORAGE_ACCOUNT_ACCESS_KEY']
    container_name = container_name
    blob_name = blob_name

    #download from blob
    blob_service = BlockBlobService(account_name = storage_account_name, account_key = storage_account_key)
    blob_string = blob_service.get_blob_to_text(container_name, blob_name).content
    logging.info(f"Config file blob is read")

    return blob_string

def parse_yaml_string(yaml_string):
    #parse string
    yaml_obj = yaml.load(StringIO(yaml_string))
    logging.info(f"Config file yaml text is parsed\n"
                 f"{yaml_obj}")

    return yaml_obj

# read cfg files
def read_xref_cfg(in_blob_name, master_config_container_name, master_config_blob_name, storage_account_name, storage_account_key):

    blob_string = read_blob_as_string(master_config_container_name, master_config_blob_name, storage_account_name, storage_account_key)
    yaml_obj = parse_yaml_string(blob_string)

    for fc in yaml_obj['inbound_files']:
        blob_regex_pattern = fc['blob_regex_pattern']
        match = re.fullmatch(blob_regex_pattern, in_blob_name)

        if match:
            logging.info(f"Xref config lookup found a match\n"
                         f"{fc}")
            return fc

    logging.info(f"Xref config lookup failed to find a match")
    return None

def read_file_cfg(file_config_container_name, file_config_blob_name, storage_account_name, storage_account_key):

    blob_string = read_blob_as_string(file_config_container_name, file_config_blob_name, storage_account_name, storage_account_key)
    yaml_obj = parse_yaml_string(blob_string)

    fc = yaml_obj

    logging.info(f"File config is read\n"
                 f"{fc}")
    return fc


# key valult
def get_credential():
    # Acquire a credential object
    credential = DefaultAzureCredential()

    return credential

def get_secret_client():
    # Acquire the resource URL
    vault_url = os.environ["KEY_VAULT_URL"]

    # Acquire a credential object
    credential = get_credential()

    # Acquire a client object
    secret_client = SecretClient(vault_url=vault_url, credential=credential)

    return secret_client

# adf
def print_activity_run_details(activity_run):
    """Print activity run details."""
    logging.info("\n\tActivity run details\n")
    logging.info("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        logging.info("\tOutput: {}".format(str(activity_run.output)))
        #logging.info("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        #logging.info("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        #logging.info("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        logging.info("\tErrors: {}".format(activity_run.error['message']))

# get activity function name from cfg
def get_activity_func_name(task_type, tasks) -> str:
    for task in tasks:
        if task['type'] == task_type:
            return task['activity_function']

# get orchestration function name from cfg
def get_orchestration_func_name(task_type, tasks) -> str:
    for task in tasks:
        if task['type'] == task_type:
            return task['orchestration_function']

# merge dict
def merge(a, b, path=None, update=True) -> dict:
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            elif isinstance(a[key], list) and isinstance(b[key], list):
                for idx, val in enumerate(b[key]):
                    a[key][idx] = merge(a[key][idx], b[key][idx], path + [str(key), str(idx)], update=update)
            elif update:
                a[key] = b[key]
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a
