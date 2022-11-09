#!/usr/bin/env python3

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

import json
import logging
import os
import requests
import subprocess
import sys
import uuid

from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from requests_gssapi import HTTPSPNEGOAuth

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s: %(message)s"
)

# Global dictionary to store all environment variables.
ENV_VARIABLES = {}

commands = ["debug", "run", "seed", "test", "snapshot"]
docs = ["docs"]


def main():
    args = sys.argv
    print(args)
    if len(args) <= 1:
        print("usage: yarn_dbt [run|debug|seed|test|snapshot|docs]")
        sys.exit(10)

    # load and fetch the environment variables
    load_fetch_environment_variables()

    if sys.argv[1] in commands:
        print("Running dbt commands: ")
        launch_yarn_container_with_dbt_command()

    elif sys.argv[1] in docs:
        print("Running dbt_docs: ")
        host_dbt_docs()
    else:
        print("Option not supported" + sys.argv[1])


def load_fetch_environment_variables():
    # load the environment variables from .env file
    logging.info("Loading environment variables.")
    load_dotenv(find_dotenv())
    logging.info("Done Loading environment variables.")

    # fetch the environment variables
    ENV_VARIABLES["dbt_profiles_path"] = os.getenv("DBT_PROFILES_PATH")
    ENV_VARIABLES["yarn_jar"] = os.getenv("YARN_JAR")
    ENV_VARIABLES["dbt_user"] = os.getenv("DBT_USER")
    ENV_VARIABLES["dbt_user_keytab"] = os.getenv("DBT_USER_KEYTAB")
    ENV_VARIABLES["dbt_principal"] = os.getenv("DBT_PRINCIPAL")
    ENV_VARIABLES["git_project_name"] = os.getenv("GIT_PROJECT_NAME")
    ENV_VARIABLES["dependencies_package_location"] = os.getenv(
        "DEPENDENCIES_PACKAGE_LOCATION"
    )
    ENV_VARIABLES["dbt_project_path"] = os.getenv("DBT_PROJECT_PATH")
    ENV_VARIABLES["yarn_principal"] = os.getenv("YARN_PRINCIPAL")
    ENV_VARIABLES["yarn_keytab"] = os.getenv("YARN_KEYTAB")
    ENV_VARIABLES["yarn_rm_uri"] = os.getenv("YARN_RM_URI")



def generate_yarn_shell_command():
    global sys_args

    kinit_command = "kinit -kt {} {}".format(
        ENV_VARIABLES["dbt_user_keytab"], ENV_VARIABLES["dbt_principal"]
    )

    working_dir = "/tmp/dbt-{}".format(datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"))

    create_working_dir_command = "mkdir -p {} && tar -zxf {} --directory {} && cd {} && python3 -m venv {}/dbt-venv".format(
        working_dir,
        "dbt-workspace.tar.gz",
        working_dir,
        working_dir,
        working_dir,
    )

    download_python_dependencies_from_hdfs = "hdfs dfs -copyToLocal {}/dependencies.tar.gz {} && tar -zxf {}/dependencies.tar.gz --directory {}".format(
        ENV_VARIABLES["dependencies_package_location"],
        working_dir,
        working_dir,
        working_dir,
    )

    populate_working_dir_command = "source {}/dbt-venv/bin/activate && cd {}/dependencies && {}/dbt-venv/bin/pip install * -f ./ --no-index".format(
        working_dir,
        working_dir,
        working_dir,
    )

    dbt_command_string = " ".join(sys.argv[1:])
    dbt_command = "cd {}/{} && {}/dbt-venv/bin/dbt {} --profiles-dir={}/{}".format(
        working_dir,
        ENV_VARIABLES["git_project_name"],
        working_dir,
        command,
        working_dir,
        ENV_VARIABLES["git_project_name"],
    )

    dbt_logs_command = "cat logs/dbt.log >&2"

    # commands are meant to sequentially after previous success except dbt_logs_command that runs regardless of dbt_command success/failure.
    shell_command = "{} && {} && {} && {} && {} ; {}".format(
        kinit_command,
        create_working_dir_command,
        download_python_dependencies_from_hdfs,
        populate_working_dir_command,
        dbt_command,
        dbt_logs_command,
    )

    return shell_command


def get_yarn_app_id(app_name):
    # fetch all yarn applications
    yarn_apps_list = subprocess.run(
        [
            "yarn",
            "application",
            "-list",
            "-appStates",
            "ALL",
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )

    # filter the current application from list of all yarn apps
    grep_appId = subprocess.run(
        ["grep", app_name],
        input=yarn_apps_list.stdout,
        stdout=subprocess.PIPE,
        check=True,
        text=True,
    )

    # get the yarn application id
    yarn_id = grep_appId.stdout.split()[0]
    logging.debug("Yarn application id: %s", yarn_id)
    return yarn_id


# fetch and print the terminal output for the dbt command invoked.
def print_yarn_logs(yarn_id, log_type):
    yarn_logs = subprocess.run(
        ["yarn", "logs", "-applicationId", yarn_id, "-log_files", log_type],
        check=True,
        capture_output=True,
        text=True,
    )
    print(yarn_logs.stdout)


# Compress current git project to localize in yarn containers.
def compress_project_directory():
    logging.info("Compressing dbt project directory: %s", os.getcwd())
    compressed_project_directory = "/tmp/dbt-workspace.tar.gz"
    result = subprocess.run(
        [
            "tar",
            "-zcf",
            compressed_project_directory,
            ENV_VARIABLES["git_project_name"],
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    logging.info("Done compressing dbt project directory.")


def launch_yarn_container_with_dbt_command():
    compress_project_directory()

    # generate unique app name based on timestamp,username and host mac id.
    current_time = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    app_name = "dbt.{}.{}.{}".format(
        ENV_VARIABLES["dbt_user"], uuid.uuid1(), current_time
    )
    shell_command = generate_yarn_shell_command()
    logging.info("shell command generated: %s", shell_command)
    logging.info(
        "Starting to execute the DBT job in YARN using Distributed Shell App for appid: %s",
        app_name,
    )

    try:
        result = subprocess.run(
            [
                "hadoop",
                "org.apache.hadoop.yarn.applications.distributedshell.Client",
                "-jar",
                ENV_VARIABLES["yarn_jar"],
                "-container_memory",
                "2048",
                "-localize_files",
                "/tmp/dbt-workspace.tar.gz",
                "-appname",
                app_name,
                "-shell_command",
                shell_command,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        yarn_id = get_yarn_app_id(app_name)
        # fetch std errror and print to console in case of dbt/yarn run failure
        print_yarn_logs(yarn_id, "prelaunch.err")

    # else print to console the output from dbt
    yarn_id = get_yarn_app_id(app_name)
    print_yarn_logs(yarn_id, "prelaunch.out")


# Upload dbt project to hdfs
def copy_project_to_hdfs():
    compressed_project_directory = "/tmp/dbt-workspace.tar.gz"
    subprocess.run(
        [
            "hdfs",
            "dfs",
            "-copyFromLocal",
            "-f",
            compressed_project_directory,
            "/tmp",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    logging.info("Done Uploading dbt project to hdfs.")


# generate JSON payload dynamically to send to yarn container to generate /serve dbt docs
def generate_yarn_payload():

    kerberos_principal = {}
    kerberos_principal["principal_name"] = "{}".format(ENV_VARIABLES["yarn_principal"])
    kerberos_principal["keytab"] = "file://{}".format(ENV_VARIABLES["yarn_keytab"])

    copy_project_to_hdfs()

    artifact = {}
    artifact["type"] = "TARBALL"
    artifact["id"] = "hdfs:///tmp/dbt_profiles.tar.gz"

    component = {}
    component["name"] = "dbtdocs"
    component["number_of_containers"] = 1

    yarn_local_working_dir = "/tmp/dbt-{}".format(
        datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    )

    create_working_dir_command = (
        "mkdir -p {} && cd {} && python3 -m venv {}/dbt-venv".format(
            yarn_local_working_dir,
            yarn_local_working_dir,
            yarn_local_working_dir,
            yarn_local_working_dir,
        )
    )

    download_python_dependencies_from_hdfs = "hdfs dfs -copyToLocal {}/dependencies.tar.gz {} && tar -zxf {}/dependencies.tar.gz --directory {}".format(
        ENV_VARIABLES["dependencies_package_location"],
        yarn_local_working_dir,
        yarn_local_working_dir,
        yarn_local_working_dir,
    )

    populate_working_dir_command = "source {}/dbt-venv/bin/activate && cd {}/dependencies && {}/dbt-venv/bin/pip install * -f ./ --no-index".format(
        yarn_local_working_dir,
        yarn_local_working_dir,
        yarn_local_working_dir,
    )

    download_dbt_project_from_hdfs = "hdfs dfs -copyToLocal /tmp/dbt-workspace.tar.gz {} && tar -zxf {}/dbt-workspace.tar.gz --directory {}".format(
        yarn_local_working_dir,
        yarn_local_working_dir,
        yarn_local_working_dir,
    )

    generate_serve_dbt_docs = "cd {}/{} && {}/dbt-venv/bin/dbt docs generate --profiles-dir={}/{} ; echo 'DBT docs hosted on port 7777 on host: ' $(hostname) >&2 && python3 -m http.server 7777 --directory target".format(
        yarn_local_working_dir,
        ENV_VARIABLES["git_project_name"],
        yarn_local_working_dir,
        yarn_local_working_dir,
        ENV_VARIABLES["git_project_name"],
    )

    launch_command = "{} && {} && {} && {} && {}".format(
        create_working_dir_command,
        download_python_dependencies_from_hdfs,
        populate_working_dir_command,
        download_dbt_project_from_hdfs,
        generate_serve_dbt_docs,
    )

    print(launch_command)

    component["launch_command"] = launch_command

    component["resource"] = {"cpus": 1, "memory": "512"}

    env = {}
    configuration = {}
    configuration["env"] = env
    component["configuration"] = configuration

    payload = {}
    payload["name"] = "dbt-service"
    payload["version"] = "1.0"
    payload["kerberos_principal"] = kerberos_principal
    payload["artifact"] = artifact
    payload["components"] = [component]
    logging.info("Payload generation done: \n%s", json.dumps(payload, indent=2))
    return payload


# host dbt docs on yarn container
def host_dbt_docs():
    payload = generate_yarn_payload()
    headers = {"Content-Type": "application/json"}
    gssapi_auth = HTTPSPNEGOAuth()

    # Rest Api doc: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/yarn-service/YarnServiceAPI.html#ConfigFile
    response = requests.post(
        ENV_VARIABLES["yarn_rm_uri"] + "/app/v1/services",
        data=json.dumps(payload),
        auth=gssapi_auth,
        headers=headers,
        verify=False,
    )
    print(response.text)

