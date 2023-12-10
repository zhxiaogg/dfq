# DFQ Tool

A CLI tool for running SQLs over various data sources.

## Usage

```
# dfq --help
A CLI for running SQLs over various data sources.

Usage: dfq [OPTIONS] [DATA_AND_SQL]...

Arguments:
  [DATA_AND_SQL]...  data sources and SQL, e.g. `sample.csv "select * from t0"`

Options:
  -d, --dialect <DIALECT>  
  -o, --output <OUTPUT>    [default: terminal] [possible values: json, csv, terminal]
  -h, --help               Print help
# dfq samples/users.csv samples/orders.csv "select count(*) as num_orders, t0.name from t0 join t1 on t0.id = t1.user group by t0.name order by num_orders"
+------------+--------+
| num_orders | name   |
+------------+--------+
| 1          | Henry  |
| 2          | Taylor |
+------------+--------+
```

## Status
### Supported Data Sources
1. Local line delimeted JSON file, ends with `.json` or `.json.gz`
1. (TODO) Local JSON array file
1. Local CSV file, ends with `.csv` or `.csv.gz`
1. Parquet file, ends with `.parquet` or `.prq`

### Supported Output Formats
1. Printed table format (default)
1. JSON array format
1. JSON line delimeted format
1. CSV
1. Parquet

All outputs are directed to stdout now, need the user to manually pipe them to a file if needed.

