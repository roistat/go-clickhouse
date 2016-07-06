# go-clickhouse [![Travis status](https://api.travis-ci.org/roistat/go-clickhouse.svg)](https://travis-ci.org/roistat/go-clickhouse) [![Coverage Status](https://coveralls.io/repos/github/roistat/go-clickhouse/badge.svg)](https://coveralls.io/github/roistat/go-clickhouse)

Golang [Yandex ClickHouse](https://clickhouse.yandex/) connector

ClickHouse manages extremely large volumes of data in a stable and sustainable manner.
It currently powers Yandex.Metrica, world’s second largest web analytics platform,
with over 13 trillion database records and over 20 billion events a day, generating
customized reports on-the-fly, directly from non-aggregated data. This system was
successfully implemented at CERN’s LHCb experiment to store and process metadata on
10bn events with over 1000 attributes per event registered in 2011.

## Examples

#### Query rows

```go
conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHttpTransport())
query := clickhouse.NewQuery("SELECT name, date FROM clicks")
iter := query.Iter(conn)
var (
    name string
    date string
)
for iter.Scan(&name, &date) {
    //
}
```

#### Single insert
```go
conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHttpTransport())
query, err := clickhouse.BuildInsert("clicks",
    clickhouse.Columns{"name", "date"},
    clickhouse.Row{"Test name", "2016-01-01 21:01:01"},
)
if err != nil {
    err = query.Exec(conn)
    if err != nil {
        // Success
    }
}
```

#### Multiple connections

Cluster pings connections so you can use `cluster.ActiveConn()` to get random active connection. It might
be useful when you use several `Distributed` tables.

```go
http := clickhouse.NewHttpTransport()
conn1 := clickhouse.NewConn("host1", http)
conn2 := clickhouse.NewConn("host2", http)

cluster := clickhouse.NewCluster(conn1, conn2)

// Ping connections every second
go func() {
    for {
        cluster.Ping()
        time.Sleep(time.Second)
    }
}()
```
