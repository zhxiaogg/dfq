# DFQ Tool

A command line tool for running SQLs over various data sources.

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
