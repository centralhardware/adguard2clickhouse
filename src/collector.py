import os
import select
import subprocess
import time
from typing import Iterator

import clickhouse_connect
import json5
from dateutil import parser

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_DATABASE")
clickhouse = clickhouse_connect.get_client(host=host, database=database, port=8123, username=user, password=password)
tail_file = os.getenv("TAIL_FILE")

def follow(file, sleep_sec=0.1) -> Iterator[str]:
    """ Yield each line from a file as they are written.
    `sleep_sec` is the time to sleep after empty reads. """
    line = ''
    while True:
        tmp = file.readline()
        if tmp is not None and tmp != "":
            line += tmp
            if line.endswith("\n"):
                yield line
                line = ''
        elif sleep_sec:
            time.sleep(sleep_sec)

if __name__ == '__main__':
    with open("/code/querylog.log", 'r') as file:
        for line in follow(file):
            print(line, end='')
            j = json5.loads(line)
            try:
                isFiltered = j['Result']['IsFiltered']
            except KeyError:
                isFiltered = False

            try:
                upstream = j['Upstream']
            except KeyError:
                upstream = ''

            try:
                cached = j['Cached']
            except KeyError:
                cached = False

            data = [[parser.isoparse(j['T']), j['QH'], j['QT'], j['QC'], j['CP'], upstream, j['Answer'], j['IP'], isFiltered, j['Elapsed'], cached]]
            clickhouse.insert('log', data,
                              ['date_time', 'QH', 'QT', 'QC', 'CP', 'Upstream', 'Answer', 'IP', 'IsFiltered','Elapsed', 'Cached'])