import csv
import glob
from os import path
from datetime import datetime
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Don't forget to add a '/' at the end
LOGS_FILES_DIR = "/torque_logs/"

# InfluxDB2 config
INFLUXDB2_URL = "http://localhost:8086"
INFLUXDB2_TOKEN = "token"
INFLUXDB2_ORG = "users"
INFLUXDB2_BUCKET = "torque"
INFLUXDB2_MEASUREMENT = "torque"

# Flux query to get all existing filenames
GET_FILENAMES_QUERY = (
    'import "influxdata/influxdb/schema"\
    schema.tagValues(bucket: "'
    + INFLUXDB2_BUCKET
    + '", tag: "filename", start: -10y, stop: now())'
)

GPS_TIME = "GPS Time"


def is_header_row(row):
    """Checks if the passed in csv row is a 'header' row.
    This is required because sometimes Torque log files seem to repeat the header row in the same file multiple times.
    One way to do this is to check if the first element in the array is "GPS TIME"."""

    return row.get(GPS_TIME) == GPS_TIME


def flux_tag_keys_query_result_to_array(query_result):
    """Convert the result of the flux query to get all tag keys into an array"""
    existing_files = []
    for table in query_result:
        for record in table.records:
            existing_files.append(record.get_value())
    return existing_files


client = InfluxDBClient(url=INFLUXDB2_URL, token=INFLUXDB2_TOKEN, org=INFLUXDB2_ORG)

write_api = client.write_api(write_options=SYNCHRONOUS)

torque_log_files = glob.glob(LOGS_FILES_DIR + "*.csv")

tag_keys = client.query_api().query(org=INFLUXDB2_ORG, query=GET_FILENAMES_QUERY)
existing_files = flux_tag_keys_query_result_to_array(tag_keys)

COUNTER = 1

for torque_log_file in torque_log_files:

    print(
        f"Processing: {str(torque_log_file)} ({str(COUNTER)}/{str(len(torque_log_files))})"
    )

    if path.basename(torque_log_file) in existing_files:
        # print("Skipping, file already processed.")
        COUNTER += 1
        continue

    with open(torque_log_file, mode="r", encoding="utf-8") as file:

        csv_file = csv.DictReader(file)

        for row in csv_file:
            time = row.get(GPS_TIME)

            # If time is not set, no point in processing this row since InfluxDB REQUIRES a time filed
            if time == "-" or time is None:
                continue

            # Sometimes the header row will repeat at random places, skip those
            if is_header_row(row):
                continue

            field_dict = {}
            for key, value in row.items():
                if value == "-" or value == "∞" or value is None:
                    row[key] = 0
                if "Time" in key:
                    field_dict[key.strip()] = value
                else:
                    field_dict[key.strip()] = float(row[key])

            write_api.write(
                INFLUXDB2_BUCKET,
                INFLUXDB2_ORG,
                [
                    {
                        "measurement": INFLUXDB2_MEASUREMENT,
                        "tags": {"filename": path.basename(torque_log_file)},
                        "time": datetime.strptime(
                            time, "%a %b %d %H:%M:%S %Z%z %Y"
                        ).isoformat(),
                        "fields": field_dict,
                    }
                ],
            )

    COUNTER += 1

write_api.close()
client.close()
