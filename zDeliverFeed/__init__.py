import json
import logging

import azure.functions as func

def generate_data(business_date, num_records):
    records = []
    delim = '|'
    # header record
    char_col = f"CHAR_COL"
    varchar_col = f"VARCHAR_COL"
    bit_col = f"BIT_COL"
    tinyint_col = f"TINYINT_COL"
    smallint_col = f"SMALLINT_COL"
    int_col = f"INT_COL"
    bigint_col = f"BIGINT_COL"
    decimal_col = f"DECIMAL_COL"
    numeric_col = f"NUMERIC_COL"
    date_col = f"DATE_COL"
    datetime_col = f"DATETIME_COL"
    record = f"{char_col}{delim}{varchar_col}{delim}{bit_col}{delim}{tinyint_col}{delim}{smallint_col}{delim}{int_col}{delim}{bigint_col}{delim}{decimal_col}{delim}{numeric_col}{delim}{date_col}{delim}{datetime_col}"
    records.append(record)
    
    # data records
    for i in range(num_records):
        char_col = f"Azure Functions PoC"
        varchar_col = f"Test File Record {i + 1}"
        bit_col = f"{i % 2}"
        tinyint_col = f"{i % 256}"
        smallint_col = f"{i % 32768}"
        int_col = f"{i % 100000000}"
        bigint_col = f"{i % 10000000000}"
        decimal_col = f"{i / 10000}"
        numeric_col = f"{i / 10000000000:.10f}"
        date_col = business_date
        date_str = datetime.datetime.utcnow().isoformat(sep=' ', timespec='milliseconds')
        datetime_col = f"{date_str}"

        record = f"{char_col}{delim}{varchar_col}{delim}{bit_col}{delim}{tinyint_col}{delim}{smallint_col}{delim}{int_col}{delim}{bigint_col}{delim}{decimal_col}{delim}{numeric_col}{delim}{date_col}{delim}{datetime_col}"
        records.append(record)

    data = "\n".join(records)
    return data

def main(event: func.EventGridEvent, outputBlob: func.Out[str]):
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

    file_name = "out_sample"
    file_ext = "dat"
    business_date = "20210529"
    num_records_str = "10"

    if not file_name or not file_ext or not business_date:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            file_name = "out_sample"
            file_ext = "dat"
            business_date = "20210529"
            num_records_str = "10"

    business_date_yyyy_mm_dd = f"{business_date[:4]}-{business_date[4:6]}-{business_date[6:]}"
    num_records = int(num_records_str)
    full_file_name = f"{file_name}_{business_date}.{file_ext}"

    if file_name and file_ext and business_date:
        if not num_records:
            num_records = 50

        output = generate_data(business_date_yyyy_mm_dd, num_records)
        outputBlob.set(output)

