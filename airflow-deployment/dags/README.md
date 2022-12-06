Sample Dag to work with the dbt example projects (ex. https://github.com/cloudera/dbt-spark-livy-example.git)

Prerequisite:
1. Create a connnection object from Airflow UI with the necessary connection parameter values are required by the profiles.yml for setting up connections.
   (Ex. https://github.com/cloudera/dbt-spark-livy-example/blob/main/dbt_spark_livy_demo/profiles.yml)
2. Place the DAG file in the path used by the Airflow installation to reference DAGS.

Execute the DAG to go through the forllowing 
- dbt --version
- dbt debug
- dbt seed
- dbt run
- dbt test

