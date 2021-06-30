import os
import shutil
import time
import re

DATA_DIR = "."

def main():
    anonimize_logs(DATA_DIR)

def anonimize_logs(log_path):
    """
    Repair
    """
    print("(1/1) Repair logs")
    if os.path.isfile(log_path + '/processed_fixed.log'):
        while True:
            overWrite = input(
                "Already found a processed_fixed.log, would you like to overwrite the file? Y = yes, N = no\n")
            if overWrite == "Y":
                print("Overwriting...")
                os.remove(log_path + '/processed_fixed.log')
                break
            if overWrite == "N":
                print("Skipping repairing of log files...")
                return
            else:
                print("Invalid input")

    with open(log_path + '/processed_fixed.log', 'w') as wfd:
        with open(log_path + '/processed.log') as infile:
            for line in infile:
                line = re.sub(r"(\sOFFSET\s+['][@':.+\-a-zA-Z0-9_\s]*['])", r"", line)
                line = re.sub(r"(\sOFFSET\s+[][0-9])", r"", line)
                try:
                    split = line.split("FROM")
                    first = re.sub(
                        r"(\sAS\s+[`][@':.+\-a-zA-Z0-9_\s]*[`])", r"", split[0])
                    line = first + "FROM" + split[1]
                except:
                    print(split)
                if len(line) > 7:
                    wfd.write(line)


if __name__ == '__main__':
    main()
