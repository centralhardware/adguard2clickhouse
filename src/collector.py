import base64
from netaddr import valid_ipv4
import os
import time
from typing import Iterator

import clickhouse_connect
import json5
from dateutil import parser
from dnslib import DNSRecord, A

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_DATABASE")
clickhouse = clickhouse_connect.get_client(host=host, database=database, port=8123, username=user, password=password)
table = os.getenv("TABLE")


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
    print("starting")
    open("/code/querylog.log", 'w').close()
    with open("/code/querylog.log", 'r') as file:
        for line in follow(file):
            j = json5.loads(line)

            date_time = parser.isoparse(j['T'])
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

            t = DNSRecord.parse(base64.b64decode(j['Answer']))
            rdata = t.a.rdata
            if rdata is None or not valid_ipv4(str(rdata)):
                rdata = None

            data = [[date_time, j['QH'], j['QT'], j['QC'], j['CP'], upstream,j['Answer'], j['IP'], isFiltered, j['Elapsed'], cached, str(rdata), t.header.rcode]]
            clickhouse.insert(table, data,
                              ['date_time', 'QH', 'QT', 'QC', 'CP', 'Upstream', 'Answer', 'IP', 'IsFiltered','Elapsed', 'Cached', 'rdata', 'rcode'])