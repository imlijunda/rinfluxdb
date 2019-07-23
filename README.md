# Overview

This is an R package to query and write data to InfluxDB.

# Install

```r
devtools::install_github("imlijunda/rinfluxdb")
```

# Using rinfluxdb

```r
library(rinfluxdb)

# create a connection
con <- influxdb(host = "localhost", port = 8086, user = "user", pass = "notmypass!")

# write data
influxdb_write(data, con, db = "testdb", measurement = "testm", tag = "id")

# or
con$write(data, db = "testdb")

# query data
v <- influxdb_query("select * from testm where id='dummy_id'", con, db = "testdb")

# or
con$query(q = "select * from testm where id='dummy_id'", db = "testdb")

# converting data to InfluxDB line protocol
lp <- line_protocol(data, measurement = "testm", tag = "id", do_escape = TRUE, force_integer = TRUE, epoch = "u")
```

# Remarks

This package is still under heavy development and features are expected to change
in the near future.
