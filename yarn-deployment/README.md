# yarn-deployment

The `dbt-hive` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Hive, Impala and Spark] leaveraging apache yarn as the underlying resource manager.


## Getting started

- 


### Requirements

Python >= 3.8
dbt-core ~= 1.1.0
impyla >= 0.18

### Install
```
pip3 install cloudera-dbt-deployment
```

### Sample profile
```
demo_project:
  target: dev
  outputs:
  dev:
    type: [hive/impala/spark-livy]
    auth_type: kerberos
    schema: [schema]
    host: [hostname]
    port: 10000
    http_path: [http-path] #optional
    use_http_transport: [true/false]
    use_ssl: [true/false]
    thread: 1
```
