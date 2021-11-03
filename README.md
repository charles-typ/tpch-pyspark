# TPC-H PySpark

TPC-H benchmark implemented in PySpark (Spark 3.2.0 with in-built Hadoop 2.7) using the DataFrames API.


## Generating tables

Under the dbgen directory do:
```
make
```

This should generate an executable called `dbgen`
```
./dbgen -h
```
gives you the various options for generating the tables. 

To generate tables, the simplest case is running:
```
./dbgen
```
which generates tables with extension `.tbl` with scale 1 (default) for a total of rougly 1GB size across all tables. For different size tables you can use the `-s` option:
```
./dbgen -s 10
```
will generate roughly 10GB of input data.

You can then either upload your data to hdfs or read them locally.