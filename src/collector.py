import base64
import logging
import os

import clickhouse_connect
import json5
import tailer
from dateutil import parser
from dnslib import DNSRecord
from netaddr import valid_ipv4, valid_ipv6

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_DATABASE")
clickhouse = clickhouse_connect.get_client(host=host, database=database, port=8123, username=user, password=password,
                                           settings={'async_insert': '1', 'wait_for_async_insert': '0'})
table = os.getenv("TABLE")


def process_line(line):
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

    blockedBy = []
    try:
        for result in j['Result']:
            blockedBy.append(int(result['FilterListID']))
    except KeyError:
        blockedBy = None

    t = DNSRecord.parse(base64.b64decode(j['Answer']))
    rdatas = []
    rdatas6 = []
    cnames = []
    for pr in t.rr:
        if pr.rdata is not None and valid_ipv4(str(pr.rdata)):
            rdatas.append(str(pr.rdata))
        elif pr.rdata is not None and valid_ipv6(str(pr.rdata)):
            rdatas6.append(str(pr.rdata))
        elif pr.rdata is not None:
            cnames.append(str(pr.rdata))

    data = [
        [date_time, j['QH'], j['QT'], j['QC'], j['CP'], upstream, j['Answer'], j['IP'], isFiltered, j['Elapsed'],
         cached, t.header.rcode, rdatas, rdatas6, cnames, blockedBy]]
    clickhouse.insert(table, data,
                      ['date_time', 'QH', 'QT', 'QC', 'CP', 'Upstream', 'Answer', 'IP', 'IsFiltered', 'Elapsed',
                       'Cached', 'rcode', 'rdatas', 'rdatas6', 'cnames', 'blockedBy2'])


if __name__ == '__main__':
    open("/code/querylog.log", 'w').close()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.info('start application')

    for line in tailer.follow(open("/code/querylog.log")):
        try:
            process_line(line)
        except Exception as e:
            logging.error(e)
