import base64
import json
import logging
import os
import traceback

import clickhouse_connect
import tailer
from dnslib import DNSRecord
from netaddr import valid_ipv4, valid_ipv6


class DNSQuery:
    def __init__(self, dns_query):
        self.log = json.loads(dns_query)
        self.date_time = self.log['T']
        self.query_address = self.log['QH']
        self.query_type = self.log['QT']
        self.query_class = self.log['QC']
        self.client_proto = self.log['CP']
        self.upstream_addr = self.log.get('Upstream', '')
        self.ip_address = self.log['IP']
        self.is_filtered = self.log.get('Result', {}).get('IsFiltered', False)
        self.elapsed = self.log['Elapsed']
        self.is_cached = self.log.get('Cached', False)
        self.r_code, self.r_datas, self.r_datas6, self.c_names = self.parse_dns_record()

    def parse_dns_record(self):
        t = DNSRecord.parse(base64.b64decode(self.log['Answer']))
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
        return t.header.rcode, rdatas, rdatas6, cnames


def main():
    open("/code/querylog.log", 'w').close()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.info('start application')
    config = get_config()

    clickhouse = clickhouse_connect.get_client(host=config['host'], database=config['database'], port=8123,
                                               username=config['username'], password=config['password'],
                                               settings={'async_insert': '1', 'wait_for_async_insert': '0'})

    for line in tailer.follow(open("/code/querylog.log")):
        try:
            dns_query = DNSQuery(line)
            data = [[
                dns_query.date_time,
                dns_query.query_address,
                dns_query.query_type,
                dns_query.query_class,
                dns_query.client_proto,
                dns_query.upstream_addr,
                dns_query.ip_address,
                dns_query.is_filtered,
                dns_query.elapsed,
                dns_query.is_cached,
                dns_query.r_code,
                dns_query.r_datas,
                dns_query.r_datas6,
                dns_query.c_names
            ]]
            clickhouse.insert('log2', data,
                              ['date_time', 'QH', 'QT', 'QC', 'CP', 'Upstream', 'IP', 'IsFiltered', 'Elapsed',
                               'Cached', 'rcode', 'rdatas', 'rdatas6', 'cnames'])
        except Exception:
            traceback.print_exc()
            print(line)


def get_config():
    # Check if the config file exists and read data from it if it is
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE")
    return {"username": user, "password": password, "host": host, "database": database}


if __name__ == "__main__":
    main()