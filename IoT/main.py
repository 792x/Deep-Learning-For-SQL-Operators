import os
import shutil
import time
import re

DATA_DIR = "./logs"


def main():
    """
    For each log file, reads and parses the log and anonimizes the query parameters
    """
    merge_postgres_logs(DATA_DIR)
    anonimize_logs(DATA_DIR)


def merge_postgres_logs(log_path):
    """
    Reads the postgres logs and merges them into a single file, i.e. combines all days
    into one big ordered log.
    """
    print("(1/2) Merging log files")
    if os.path.isfile(log_path + '/combined.log'):
        while True:
            overWrite = input(
                "Already found a combined.log, would you like to overwrite the file? Y = yes, N = no\n")
            if overWrite == "Y":
                print("Overwriting...")
                os.remove(log_path + '/combined.log')
                break
            if overWrite == "N":
                print("Skipping merging of log files...")
                return
            else:
                print("Invalid input")

    filenames = sorted(
        [filename for filename in os.listdir(
            path=log_path) if filename.endswith(".log") and filename != "combined.log"],
        key=lambda x: time.mktime(time.strptime(os.path.splitext(x)[0][11:-5], "%Y-%m-%d")))
    with open(log_path + '/combined.log', 'wb') as wfd:
        for f in filenames:
            with open(log_path + '/' + f, 'rb') as fd:
                shutil.copyfileobj(fd, wfd)


def anonimize_logs(log_path):
    # strings [=|<|>]\s+(['][@':.+\-a-zA-Z0-9_]+['])              ([=|<|>]\s+)(['][@':.+\-a-zA-Z0-9_]+['])
    # nums [=|<|>]\s+([^'$][0-9.,]+[^')])
    print("(2/2) Anonimizing logs")
    if os.path.isfile(log_path + '/processed.log'):
        while True:
            overWrite = input(
                "Already found a processed.log, would you like to overwrite the file? Y = yes, N = no\n")
            if overWrite == "Y":
                print("Overwriting...")
                os.remove(log_path + '/processed.log')
                break
            if overWrite == "N":
                print("Skipping anonimizing of log files...")
                return
            else:
                print("Invalid input")

    with open(log_path + '/processed.log', 'w') as wfd:
        with open(log_path + '/combined.log') as infile:
            for line in infile:
                if "statement: SELECT" in line and "pg_" not in line and not ("public." in line and "_id_seq" in line):
                    line = line.split("statement: ")[1]
                    line = re.sub(
                        r"([=|<|>]\s+)(['][@':.+\-a-zA-Z0-9_\s]*['])", r"\g<1>'0'", line)
                    line = re.sub(
                        r"([=|<|>]\s+)([0-9.,]+)", r"\g<1>0", line)
                    line = re.sub(r"(IN [(][0-9, ]*[)])", "IN (0)", line)
                    line = re.sub(r"(?<!AS )\"([\w.]+)\"", r"\g<1>", line)
                    line = re.sub(r"(?<=AS )\"([\w.]+)\"", r"`\g<1>`", line)
                    if len(line) > 7:
                        wfd.write(line)


if __name__ == '__main__':
    main()
