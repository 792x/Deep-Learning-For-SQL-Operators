import csv
import os
import shutil
import gzip
import pandas as pd
import json

DATA_DIR = "/Volumes/Media/PhoneLab Dataset/logcat"


def main():
    """
    For each device, reads and parses the SQLite log and outputs the query log and schema of the
    database
    """
    for device_name in next(os.walk(DATA_DIR))[1]:
        if not os.path.exists(device_name):
            print(f"New device: {device_name}")
            os.makedirs(device_name)
            device_data_path = DATA_DIR + "/" + device_name + "/tag/SQLite-Query-Phonelab/2015/03/"
            extract_all_in_dir(device_data_path)
            df = read_sqlite_log(device_data_path)
            schema, query_log = parse_sqlite_log(df)
            export_to_csv(schema, query_log, device_name)
        else:
            print(f"Device folder found, skipping: {device_name}")


def export_to_csv(schema, query_log, device_dir):
    """
    Writes the schema and query_log to disk at working directory in CSV format
    """
    print("(4/4) Exporting to CSV")
    if not os.path.exists(device_dir + "/data"):
        os.makedirs(device_dir + "/data")

    for k, v in schema.items():
        column_names = []
        if not os.path.exists(device_dir + "/data/" + k):
            os.makedirs(device_dir + "/data/" + k)
        with open(device_dir + "/data/" + k + "/" + k + ".csv", 'w') as out:
            csv_out = csv.writer(out, delimiter=';')
            splits = v.split("cid")
            for split in splits:
                subsplits = split.split(",")
                for subsubsplit in subsplits:
                    if "name:" in subsubsplit:
                        column_names.append(subsubsplit[5:])
            csv_out.writerow(column_names)

    with open(device_dir + "/query_log.csv", 'w') as out:
        csv_out = csv.writer(out, delimiter=';')
        csv_out.writerow(["start_timestamp", "end_timestamp", "date_time"])
        for row in query_log:
            csv_out.writerow(row)


def parse_sqlite_log(df):
    """
    Parses the SQLite log entries for SCHEMA and CRUD actions to extract the schema
    and query log
    """
    print("(3/4) Parsing SQLite log")
    schema = {}
    query_log = []
    for row in df.itertuples(index=False):
        try:
            details = json.loads(row.details)
            if details["Action"] == "SCHEMA" and details["TABLE_NAME"] not in schema:
                schema[details["TABLE_NAME"]] = details["SCHEMA"]
            elif details["Action"] in ["SELECT", "INSERT", "UPSERT", "UPDATE",
                                       "DELETE"] and "PRAGMA" not in details["Results"]:
                sqlite_program = details["Results"]
                if sqlite_program.startswith('SQLiteProgram: '):
                    sqlite_program = sqlite_program[15:]
                query_log.append(
                    (row.start_timestamp, row.end_timestamp, row.date_time,
                     sqlite_program.replace('\n', '')))
        except:
            pass
    return schema, query_log


def read_sqlite_log(device_path):
    """
    Reads the sqlite logs and returns the concatenated dataframe per device, i.e. combines all days
    into one big ordered dataframe.
    """
    print("(2/4) Reading SQLite log")
    files = sorted(
        [filename for filename in os.listdir(path=device_path) if filename.endswith(".out")],
        key=lambda x: int(os.path.splitext(x)[0]))
    df = pd.concat((pd.read_csv(device_path + filename, sep='\t', lineterminator='\n',
                                names=["device", "start_timestamp", "end_timestamp", "date_time",
                                       "unknown_1", "unknown_2", "unknown_3", "tag", "details"],
                                header=None) for filename in files), axis=0, ignore_index=True)
    return df


def extract_all_in_dir(path):
    """
    Extracts all the .gz files in a directory to that same directory if they are not already
    extracted
    """
    print(f"(1/4) Extracting archives in {path}")
    for filename in os.listdir(path=path):
        if os.path.exists(path + os.path.splitext(filename)[0]):
            continue

        if filename.endswith(".gz"):
            with gzip.open(path + filename, 'rb') as f_in:
                with open(path + os.path.splitext(filename)[0], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)


if __name__ == '__main__':
    main()
