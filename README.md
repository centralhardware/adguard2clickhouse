Save adguard home dns log to clickhouse
Main table:
```sql
create table log2
(
    date_time  DateTime,
    QH         String,
    CP         String,
    IsFiltered Bool,
    Elapsed    UInt64,
    Cached     Bool,
    rdatas     Array(IPv4),
    rdatas6    Array(IPv6),
    cnames     Array(String),
    QT LowCardinality(String),
    QC LowCardinality(String),
    Upstream LowCardinality(String),
    IP LowCardinality(IPv4),
    rcode      UInt8
)
    engine = MergeTree ORDER BY date_time
```
And i am using a number of materialized views
```sql
create table blocked_domains
(
    QH    String,
    count UInt32
)
    engine = SummingMergeTree ORDER BY QH;
CREATE MATERIALIZED VIEW AdGuardHome.blocked_domains_mv TO AdGuardHome.blocked_domains
(
    `QH` String,
    `count` UInt8
) AS
SELECT
    QH,
    1 AS count
FROM AdGuardHome.log2
WHERE IsFiltered = true;
```

```sql
-- auto-generated definition
create table clients_stats
(
    IP      String,
    visited UInt32,
    blocked UInt32
)
    engine = SummingMergeTree ORDER BY IP;
CREATE MATERIALIZED VIEW AdGuardHome.client_stats_mv TO AdGuardHome.clients_stats
(
    `IP` LowCardinality(String),
    `visited` UInt8,
    `blocked` UInt8
) AS
SELECT
    IP,
    if(IsFiltered, 0, 1) AS visited,
    if(IsFiltered, 1, 0) AS blocked
FROM AdGuardHome.log2;
```

```sql
-- auto-generated definition
create table qt_stats
(
    QT    String,
    count UInt32
)
    engine = SummingMergeTree ORDER BY QT;
CREATE MATERIALIZED VIEW AdGuardHome.qt_stats_mv TO AdGuardHome.qt_stats
(
    `QT` LowCardinality(String),
    `count` UInt8
) AS
SELECT
    QT,
    1 AS count
FROM AdGuardHome.log2;
```

```sql
-- auto-generated definition
create table rcode_stats
(
    rcode String,
    count UInt32
)
    engine = SummingMergeTree ORDER BY rcode;
CREATE MATERIALIZED VIEW AdGuardHome.rcode_stats_mv TO AdGuardHome.rcode_stats
(
    `rcode` UInt8,
    `count` UInt8
) AS
SELECT
    rcode,
    1 AS count
FROM AdGuardHome.log2;
```

```sql
-- auto-generated definition
create table stats2
(
    IP LowCardinality(IPv4),
    date_time DateTime,
    blocked   UInt32,
    visited   UInt32
)
    engine = SummingMergeTree ORDER BY (IP, date_time);
CREATE MATERIALIZED VIEW AdGuardHome.stats2_mv TO AdGuardHome.stats2
(
    `IP` LowCardinality(IPv4),
    `date_time` DateTime,
    `blocked` UInt8,
    `visited` UInt8
) AS
SELECT
    IP,
    toStartOfInterval(date_time, toIntervalMinute(10)) AS date_time,
    if(IsFiltered, 1, 0) AS blocked,
    if(IsFiltered, 0, 1) AS visited
FROM AdGuardHome.log2
ORDER BY
    IP ASC,
    date_time ASC;
```

```sql
-- auto-generated definition
create table tld_stats
(
    tld LowCardinality(String),
    count UInt32
)
    engine = SummingMergeTree ORDER BY tld
CREATE MATERIALIZED VIEW AdGuardHome.tld_stats_mv TO AdGuardHome.tld_stats
(
    `tld` String,
    `count` UInt8
) AS
SELECT
    arrayRotateRight(splitByChar('.', QH), 1)[1] AS tld,
    1 AS count
FROM AdGuardHome.log2
WHERE (length(splitByChar('.', QH)) > 1) AND (NOT isIPv4String(QH)) AND (NOT isIPv6String(QH));
```

```sql
-- auto-generated definition
create table upstream_stats
(
    Upstream LowCardinality(String),
    count UInt32
)
    engine = SummingMergeTree ORDER BY Upstream;
CREATE MATERIALIZED VIEW AdGuardHome.tld_stats_mv TO AdGuardHome.tld_stats
(
    `tld` String,
    `count` UInt8
) AS
SELECT
    arrayRotateRight(splitByChar('.', QH), 1)[1] AS tld,
    1 AS count
FROM AdGuardHome.log2
WHERE (length(splitByChar('.', QH)) > 1) AND (NOT isIPv4String(QH)) AND (NOT isIPv6String(QH));
```

```sql
-- auto-generated definition
create table visited_domains
(
    QH    String,
    count UInt32
)
    engine = SummingMergeTree ORDER BY QH;
CREATE MATERIALIZED VIEW AdGuardHome.visited_domains_mv TO AdGuardHome.visited_domains
(
    `QH` String,
    `count` UInt8
) AS
SELECT
    QH,
    1 AS count
FROM AdGuardHome.log2
WHERE IsFiltered = false;
```