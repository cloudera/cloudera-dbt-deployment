# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.configuration import conf
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook


env_vars = {
    "DBT_SPARK_LIVY_HOST": BaseHook.get_connection('dbt-spark-livy-conn').host,
    "DBT_SPARK_LIVY_SCHEMA": BaseHook.get_connection('dbt-spark-livy-conn').schema,
    "DBT_SPARK_LIVY_DBNAME": "dbtdemo",
    "DBT_SPARK_LIVY_USER": BaseHook.get_connection('dbt-spark-livy-conn').login,
    "DBT_SPARK_LIVY_PASSWORD": BaseHook.get_connection('dbt-spark-livy-conn').password,
}

# instantiate the DAG
with DAG(
    start_date=datetime(2022,6,1),
    catchup=False,
    schedule_interval='@daily',
    dag_id='dbt_spark_livy_dag_BASH_OP'
) as dag:

    version_commands="""
            virtualenv venv_dbt_af_test && source venv_dbt_af_test/bin/activate && pip install dbt-spark-livy && git clone https://github.com/cloudera/dbt-spark-livy-example.git;
            dbt --version; deactivate && rm -rf venv_dbt_af_test
            """

    debug_commands="""
            virtualenv venv_dbt_af_test && source venv_dbt_af_test/bin/activate && pip install dbt-spark-livy && git clone https://github.com/cloudera/dbt-spark-livy-example.git;
            cd dbt-spark-livy-example/dbt_spark_livy_demo && dbt debug --profiles-dir .;
            errCode=`echo $?` && cat logs/dbt.log;
            deactivate && rm -rf venv_dbt_af_test;
            if [[ $errCode != 0 ]] ;then exit $errCode;fi
            """

    seed_commands="""
            virtualenv venv_dbt_af_test && source venv_dbt_af_test/bin/activate && pip install dbt-spark-livy && git clone https://github.com/cloudera/dbt-spark-livy-example.git;
            cd dbt-spark-livy-example/dbt_spark_livy_demo && dbt seed --profiles-dir .;
            errCode=`echo $?` && cat logs/dbt.log;
            deactivate && rm -rf venv_dbt_af_test;
            if [[ $errCode != 0 ]] ;then exit $errCode;fi 
            """

    run_commands="""
            virtualenv venv_dbt_af_test && source venv_dbt_af_test/bin/activate && pip install dbt-spark-livy && git clone https://github.com/cloudera/dbt-spark-livy-example.git;
            cd dbt-spark-livy-example/dbt_spark_livy_demo && dbt run --profiles-dir .;
            errCode=`echo $?` && cat logs/dbt.log;
            deactivate && rm -rf venv_dbt_af_test;
            if [[ $errCode != 0 ]] ;then exit $errCode;fi
            """

    test_commands="""
            virtualenv venv_dbt_af_test && source venv_dbt_af_test/bin/activate && pip install dbt-spark-livy && git clone https://github.com/cloudera/dbt-spark-livy-example.git;
            cd dbt-spark-livy-example/dbt_spark_livy_demo && dbt test --profiles-dir .;
            errCode=`echo $?` && cat logs/dbt.log;
            deactivate && rm -rf venv_dbt_af_test;
            if [[ $errCode != 0 ]] ;then exit $errCode;fi
            """
    dbt_version_check = BashOperator(
        task_id='dbt_version_check',
        bash_command=version_commands,
        )

    dbt_debug_check = BashOperator(
        task_id='dbt_debug_check',
        bash_command=debug_commands,
        env=env_vars,
        append_env=True
        )

    dbt_seed_check = BashOperator(
        task_id='dbt_seed_check',
        bash_command=seed_commands,
        env=env_vars,
        append_env=True
        )

    dbt_run_check = BashOperator(
        task_id='dbt_run_check',
        bash_command=run_commands,
        env=env_vars,
        append_env=True
        )

    dbt_test_check = BashOperator(
        task_id='dbt_test_check',
        bash_command=test_commands,
        env=env_vars,
        append_env=True
        )

    dbt_version_check>>dbt_debug_check>>dbt_seed_check>>dbt_run_check>>dbt_test_check
