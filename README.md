# df-azure-serverless

### Serverless ETL Orchestration Framework in Azure
This is a serverless data processing (ETL) framework built on Azure cloud using Azure Functions, Azure Data Factory, Azure Databricks Notebooks and Azure SQL Server database. The framework is built using Azure Durable Functions in Python. It covers the data ingestion, transformation, extraction and monitoring workflows. The workflows can be configured in a declarative manner. This allows plugging in data governance/management tool sets (e.g. data quality, data reconciliation) in the workflows as needed.

![Framework Demo v2 001](https://github.com/user-attachments/assets/a8d117ca-6d6a-4938-a389-564d596121d6)

### Metadata 

##### Framework Tasks and Workflow Templates (gen_cfg.yml)

This config file contains the framework tasks available to be included in the workflows. Here, we can declare the required/optional tasks for the ingestion/extraction workflows. This becomes the templates for the respective workflows.

```
tasks:
  - type: checkFileIntegrity
    activity_function: CheckFileIntegrity
  - type: reconTotals
    activity_function: ReconTotals
  - type: checkDataQuality
    activity_function: CheckDataQuality
  - type: checkOldFile
    activity_function: CheckOldFile
  - type: ingestByPipeline
    orchestration_function: OrchestrateIngestion
    activity_function: RunPipeline
  - type: transformByPipeline
    orchestration_function: OrchestrateTransform
    activity_function: RunPipeline
  - type: extractByPipeline
    orchestration_function: OrchestrateExtraction
    activity_function: RunPipeline
  - type: logState
    activity_function: CreateLogStateEvent
  - type: createControlFile
    activity_function: CreateControlFile
  - type: createTriggerFile
    activity_function: CreateTriggerFile

pre_ingestion_tasks:
  - type: checkFileIntegrity
    run: yes
    required: yes
  - type: reconTotals
    run: no
    required: no
  - type: checkDataQuality
    run: yes
    required: no
  - type: checkOldFile
    run: no
    required: no

post_extraction_tasks:
  - type: checkDataQuality
    run: yes
    required: no
  - type: createControlFile
    run: no
    required: no
  - type: createTriggerFile
    run: no
    required: no

logging_tasks:
  - type: logState
    run: yes
    required: yes

extracts:
  storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
  internal_blob_placeholder_pattern: out-delivery/extract_business_date_run_timestamp.dat
  internal_blob_placeholders:
    - business_date: yyyymmdd
    - run_timestamp: yyyymmddhhmiss

logs:
  storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME

monitor:
  minutes_to_monitor: 10
  minutes_to_sleep: 5
```

#### Feed Registration Metadata (cfg_xref.yml)

Here, we associate the inbound and outbound file feeds to the respective ingestion/extration workflow configs.

```
inbound_files:
  - storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
    blob_regex_pattern: in\-data\-source\/sample_[0-9]{8}\.dat
    cfg: cfg1.yml

  - storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
    blob_regex_pattern: in\-data\-source\/sample2_[0-9]{8}\.dat
    cfg: cfg2.yml

  - storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
    blob_regex_pattern: in\-data\-source\/sample3_[0-9]{8}\.dat
    cfg: cfg3.yml
    
  - storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
    blob_regex_pattern: in\-data\-source\/trips_[0-9]{8}\.dat
    cfg: cfg4.yml

outbound_files:
  - cfg: cfg6.yml
```

### Ingestion Workflow

![Framework Demo v2 002](https://github.com/user-attachments/assets/7f10003c-8ee1-4461-9822-fb274606b88e)

#### Inbound File (trips_20210620.dat)

```
service_provider|bookings|cancellations
uber|100|10
ola|200|25
```

#### Ingestion Pipeline Config (cfg4.yml)

This is the ingestion workflow config. Inbound file names accept regex patterns in the file names. Here, we declare how we want the file to be ingested. i.e. by running the framework task 'ingestByPipeline'. 

```
leaf_cfg: cfg4
leaf_cfg_file_name: cfg4.yml

pattern: ingest

inbound_file:
  business_date_regex_pattern: .+_([0-9]{8})\.dat

process:
  type: ingestByPipeline
  parameters: 
    pipeline_name: loadFile
    pipeline_parameters:
      parm1: 1
      parm2: 2
```

This will invoke the following Azure functions.

```
  - type: ingestByPipeline
    orchestration_function: OrchestrateIngestion
    activity_function: RunPipeline
```

OrchestrateIngestion is an Azure durable function and so tracks the state of the workflow. RunPipeline is an Azure function which invokes the ADF pipeline to ingest the file into the database.

### Monitor Workflow

![Framework Demo v2 003](https://github.com/user-attachments/assets/00dabe19-0e59-4dfb-87d1-25c58ed6c060)

Monitor workflow allows the user to setup the dependencies for the downstream transformation and extraction workflows. For example, we can configure a transformation workflow T to wait for the ingestion workflows I1 and I2 to complete first for the given business date YYYYMMDD. These dependencies are stored in the Azure Storage Tables container for this demo. This can be housed in a database if needed.

### Transformation Workflow

![Framework Demo v2 004](https://github.com/user-attachments/assets/4b2bce1d-0d49-41ff-a71c-99b664c8d432)

This is the transformation workflow config. Here, we declare how we want the transformation to be done. i.e. by running the framework task 'transformByPipeline'. 

#### Transformation Pipeline Config (cfg5.yml)

```
leaf_cfg: cfg5
leaf_cfg_file_name: cfg5.yml

pattern: transform

process:
  type: transformByPipeline
  parameters: 
    pipeline_name: runTransform
    pipeline_parameters:
      parm1: 1
      parm2: 2
```

This will invoke the following Azure functions.

```
  - type: transformByPipeline
    orchestration_function: OrchestrateTransform
    activity_function: RunPipeline
```

OrchestrateTransform is an Azure durable function and so tracks the state of the workflow. RunPipeline is an Azure function which invokes the ADF pipeline to transform and load the data into the database.


### Extract Workflow

![Framework Demo v2 005](https://github.com/user-attachments/assets/4cf057a8-c0e9-42be-98ab-0f8f46430612)

This is the extraction workflow config. Here, we declare how we want the extraction to be done. i.e. by running the framework task 'extractByPipeline'. Also, we can configure the outbound file delivery as needed. For example, we can instruct to create a control (for recon at downstream) and trigger file (to trigger downstream process) along with the data extract.

#### Extract Pipeline Config (cfg6.yml)

```
leaf_cfg: cfg6
leaf_cfg_file_name: cfg6.yml

pattern: extract

outbound_data_file:
  storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
  internal_blob_container: out-delivery
  internal_blob_consumer_folder: consumer1
  internal_blob_placeholder_pattern: completed_trips_{business_date}.dat
  internal_blob_placeholders:
    business_date: resolve
  blob_placeholder_pattern: out-consumer/completed_trips_business_date.dat
  blob_placeholders:
    - business_date: yyyymmdd

outbound_control_file:
  storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
  blob_placeholder_pattern: out-consumer/completed_trips_business_date.ctl
  blob_placeholders:
    - business_date: yyyymmdd

outbound_trigger_file:
  storage_account_name_setting: P19_STORAGE_ACCOUNT_NAME
  blob_placeholder_pattern: out-consumer/completed_trips_business_date.rdy
  blob_placeholders:
    - business_date: yyyymmdd

process:
  type: extractByPipeline
  parameters: 
    pipeline_name: createExtract
    pipeline_parameters:
      parm1: 1
      parm2: 2
```

### Observability (Logging) Workflow

logState is an Azure function which logs the status of the user defined workflow stages in Azure Storage Tables container. This forms the basis for the observability in the framework. This functionality can be extended as per the needs.


