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
import socket
import sys
import uuid

from datetime import datetime
from dotenv import dotenv_values
from requests_gssapi import HTTPSPNEGOAuth

# default log level warning
LOGLEVEL = os.environ.get("LOGLEVEL", "WARNING").upper()

logging.basicConfig(level=LOGLEVEL, format="%(asctime)s - %(levelname)s: %(message)s")

# Global dictionary to store all environment variables.
ENV_VARIABLES = None

commands = ["debug", "run", "seed", "test", "snapshot"]
docs = ["docs"]


def main():
    args = sys.argv
    print(args)
    if len(args) <= 1:
        print("usage: yarn_dbt [run|debug|seed|test|snapshot|docs]")
        sys.exit(10)

    # load and fetch the environment variables needed for launching a yarn container
    load_fetch_environment_variables()

    if sys.argv[1] in commands:
        print("Running dbt commands: ")
        perform_user_authorization("headless_user")
        launch_yarn_container_with_dbt_command()

    elif sys.argv[1] in docs:
        print("Running dbt_docs: ")
        # perform user authorization for running yarn commands
        perform_user_authorization("service_user")
        host_dbt_docs()
    else:
        print("Option not supported: " + sys.argv[1])


# load the environment variables from yarn.env file
def load_fetch_environment_variables():
    logging.info("Loading environment variables.")
    dot_env_path = os.path.join(os.path.expanduser("~"), "yarn.env")
    isFile = os.path.isfile(dot_env_path)
    if not isFile:
        logging.critical(
            "Missing yarn.env file in home directory " + os.path.expanduser("~")
        )
        sys.exit(10)

    global ENV_VARIABLES
    ENV_VARIABLES = dotenv_values(dot_env_path)
    logging.info(f"Found config in %s", dot_env_path)
    check_environment_variables(ENV_VARIABLES)
    for key, value in ENV_VARIABLES.items():
        logging.info(f"{key} : {value}")
    logging.info("Done Loading environment variables.")


def check_environment_variables(ENV_VARIABLES):
    expected_mandatory_keys = [
        "DEPENDENCIES_PACKAGE_PATH_HDFS",
        "DEPENDENCIES_PACKAGE_NAME",
        "YARN_JAR",
        "DBT_SERVICE_USER",
        "DBT_PROJECT_NAME",
        "YARN_RM_URI",
        "DBT_HEADLESS_KEYTAB",
        "DBT_HEADLESS_PRINCIPAL",
        "CURRENT_DBT_USER",
    ]

    missing_keys = [key for key in expected_mandatory_keys if key not in ENV_VARIABLES]

    # log and exit if yarn.env file doesn't have necessary key values
    if missing_keys:
        logging.critical("Missing keys in yarn.env: %s", missing_keys)
        sys.exit(10)

    # Below keys are optional. Set values incase user doesn't add these values.
    ENV_VARIABLES.setdefault("DBT_DOCS_PORT", "7777")
    ENV_VARIABLES.setdefault("YARN_CONTAINER_MEMORY", "2048")
    ENV_VARIABLES.setdefault("YARN_TIMEOUT", "1800000")
    ENV_VARIABLES.setdefault("APPLICATION_TAGS", "test-dbt")
    

# Perform kerberos authorization in gateway machine
def perform_user_authorization(user_type):
    if user_type == "service_user":
        # get hostname
        host = socket.gethostname()
        keytab_path = get_service_user_keytab()
        logging.info("Found keytab file %s", keytab_path)

        # perform authorization for service user
        subprocess.run(
            [
                "kinit",
                "-kt",
                keytab_path,
                "{}/{}".format(ENV_VARIABLES["DBT_SERVICE_USER"], host),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        # perform authorization using headless keytab
        subprocess.run(
            [
                "kinit",
                "-kt",
                ENV_VARIABLES["DBT_HEADLESS_KEYTAB"],
                ENV_VARIABLES["DBT_HEADLESS_PRINCIPAL"],
            ],
            check=True,
            capture_output=True,
            text=True,
        )


# get the yarn service user keytab distributed to all the nodes by cloudera scm agent
def get_service_user_keytab():
    service_name = "{}.keytab".format(ENV_VARIABLES["DBT_SERVICE_USER"])
    search_path = "/var/run/cloudera-scm-agent/process/"

    # search for service keytab path
    for dirpath, dirname, filename in os.walk(search_path):
        if service_name in filename:
            return os.path.join(dirpath, service_name)

    logging.critical(
        "Couldn't find service keytab {} in location".format(service_name) + search_path
    )
    sys.exit(10)


def generate_yarn_shell_command(app_name):
    # Perform kerberos authorization inside yarn container
    kinit_start = "echo -n '{}: Kinit start: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )

    # find keytab path and hostname for gateway machine
    kinit_command = "kinit -kt {} {}".format(
        ENV_VARIABLES["DBT_HEADLESS_KEYTAB"],
        ENV_VARIABLES["DBT_HEADLESS_PRINCIPAL"],
    )
    kinit_end = "echo -n '{}: Kinit end: '; date +'%Y-%m-%d:%H:%M:%S'".format(app_name)

    # Create a scratch directory for working with dbt project in container
    working_dir = "/tmp/dbt-{}".format(datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"))
    create_working_dir_start = "echo -n '{}: Create working directory start: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )
    create_working_dir_command = "mkdir -p {} && tar -zxf {} --directory {} && cd {} && python3 -m venv {}/dbt-venv".format(
        working_dir,
        "dbt-workspace.tar.gz",
        working_dir,
        working_dir,
        working_dir,
    )
    create_working_dir_end = "echo -n '{}: Create working directory end: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )

    # Download python dependencies from HDFS to local container
    download_python_dependencies_start = "echo -n '{}: Download python dependencies start: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )
    download_python_dependencies_from_hdfs = (
        "hdfs dfs -copyToLocal {}/{} {} && tar -zxf {}/{} --directory {}".format(
            ENV_VARIABLES["DEPENDENCIES_PACKAGE_PATH_HDFS"],
            ENV_VARIABLES["DEPENDENCIES_PACKAGE_NAME"],
            working_dir,
            working_dir,
            ENV_VARIABLES["DEPENDENCIES_PACKAGE_NAME"],
            working_dir,
        )
    )
    download_python_dependencies_end = "echo -n '{}: Download python dependencies end: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )

    # Install python dependencies in local container
    populate_working_dir_command_start = "echo -n '{}: Install python dependencies start: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )
    populate_working_dir_command = "source {}/dbt-venv/bin/activate && cd {}/dependencies && {}/dbt-venv/bin/pip install * -q -f ./ --no-index".format(
        working_dir,
        working_dir,
        working_dir,
    )
    populate_working_dir_command_end = "echo -n '{}: Install python dependencies end: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )

    # Set environment variable for dbt deployment
    set_environment_variables_start = "echo -n '{}: Setting env blob for deployment start: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )

    DBT_DEPLOYMENT_ENV = {}
    DBT_DEPLOYMENT_ENV["env"] = "yarn"
    DBT_DEPLOYMENT_ENV["version"] = "1.2.0"

    dbt_env_json_string = json.dumps(DBT_DEPLOYMENT_ENV)
    set_environment_variables_command = (
        "DBT_DEPLOYMENT_ENV='{}' && export DBT_DEPLOYMENT_ENV".format(
            dbt_env_json_string
        )
    )
    set_environment_variables_end = "echo -n '{}: Setting env blob for deployment done: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )

    # Run dbt command in local container
    dbt_command_string = " ".join(sys.argv[1:])
    run_dbt_command_start = (
        "echo -n '{}: Dbt command start: '; date +'%Y-%m-%d:%H:%M:%S'".format(app_name)
    )
    dbt_command = (
        "ls -lrt {} && cd {}/{} && {}/dbt-venv/bin/dbt {} --profiles-dir={}/{}".format(
            working_dir,
            working_dir,
            ENV_VARIABLES["DBT_PROJECT_NAME"],
            working_dir,
            dbt_command_string,
            working_dir,
            ENV_VARIABLES["DBT_PROJECT_NAME"],
        )
    )
    run_dbt_command_end = (
        "echo -n '{}: Dbt command end: '; date +'%Y-%m-%d:%H:%M:%S'".format(app_name)
    )

    # Aggregate logs and cleanup workspace.
    dbt_post_run_start = "echo -n '{}: DBT post run log aggregation and cleanup start: '; date +'%Y-%m-%d:%H:%M:%S'".format(
        app_name
    )
    dbt_post_run_command = "cat logs/dbt.log >&2"
    dbt_post_run_end = "echo -n '{}: DBT post run log aggregation and cleanup end '; date +'%Y-%m-%d:%H:%M:%S'; rm -rf {}".format(
        app_name,
        working_dir,
    )

    # commands are meant to sequentially after previous success except dbt_logs_command that runs regardless of dbt_command success/failure.
    shell_command = "{} && {} && {} && {} && {} && {} && {} && {} && {} && {}  && {} && {} && {} && {} && {} && {} && {} && {} ; {} && {} && {}".format(
        kinit_start,
        kinit_command,
        kinit_end,
        create_working_dir_start,
        create_working_dir_command,
        create_working_dir_end,
        download_python_dependencies_start,
        download_python_dependencies_from_hdfs,
        download_python_dependencies_end,
        populate_working_dir_command_start,
        populate_working_dir_command,
        populate_working_dir_command_end,
        set_environment_variables_start,
        set_environment_variables_command,
        set_environment_variables_end,
        run_dbt_command_start,
        dbt_command,
        run_dbt_command_end,
        dbt_post_run_start,
        dbt_post_run_command,
        dbt_post_run_end,
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


# Compress current dbt project to localize in yarn containers.
def compress_project_directory():
    logging.info(
        "Compressing dbt project directory: %s/%s",
        os.getcwd(),
        ENV_VARIABLES["DBT_PROJECT_NAME"],
    )
    compressed_project_directory = os.path.expanduser("~/dbt-workspace.tar.gz")
    result = subprocess.run(
        [
            "tar",
            "-zcf",
            compressed_project_directory,
            ENV_VARIABLES["DBT_PROJECT_NAME"],
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    logging.info("Done compressing dbt project directory.")


def launch_yarn_container_with_dbt_command():
    # generate unique app name based on current timestamp and dbt username
    app_name = "dbt.{}.{}".format(
        ENV_VARIABLES["CURRENT_DBT_USER"],
        datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"),
    )
    compressed_project_directory = os.path.expanduser("~/dbt-workspace.tar.gz")

    logging.debug(
        "%s",
        "{}: Compress dbt project directory start: {}".format(
            app_name, datetime.utcnow().strftime("%Y-%m-%d:%H-%M-%S")
        ),
    )

    compress_project_directory()

    logging.debug(
        "%s",
        "{}: Compress dbt project directory end: {}".format(
            app_name, datetime.utcnow().strftime("%Y-%m-%d:%H-%M-%S")
        ),
    )

    shell_command = generate_yarn_shell_command(app_name)
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
                ENV_VARIABLES["YARN_JAR"],
                "-container_memory",
                ENV_VARIABLES["YARN_CONTAINER_MEMORY"],
                "-localize_files",
                compressed_project_directory,
                "-timeout",
                ENV_VARIABLES["YARN_TIMEOUT"],
                "-appname",
                app_name,
                "-application_tags",
                ENV_VARIABLES["APPLICATION_TAGS"],
                "-shell_command",
                shell_command,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        logging.critical("There was an error completing dbt command.")
        print(e)
        sys.exit(10)

    # Print to console the output from dbt
    yarn_id = get_yarn_app_id(app_name)
    print_yarn_logs(yarn_id, "prelaunch.out")
    yarn_log_string = "yarn logs -applicationId {}".format(yarn_id)
    print("To display all yarn container logs run command: ")
    print(yarn_log_string, "\n")


# Upload dbt project to hdfs
def copy_project_to_hdfs():
    compressed_project_directory = os.path.expanduser("~/dbt-workspace.tar.gz")
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
    service_user_keytab = get_service_user_keytab()
    kerberos_principal["keytab"] = "file://{}".format(service_user_keytab)
    host = socket.gethostname()
    principal = "{}/{}"
    kerberos_principal["principal_name"] = "{}/{}".format(
        ENV_VARIABLES["DBT_SERVICE_USER"], host
    )
    copy_project_to_hdfs()

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

    download_python_dependencies_from_hdfs = (
        "hdfs dfs -copyToLocal {}/{} {} && tar -zxf {}/{} --directory {}".format(
            ENV_VARIABLES["DEPENDENCIES_PACKAGE_PATH_HDFS"],
            ENV_VARIABLES["DEPENDENCIES_PACKAGE_NAME"],
            yarn_local_working_dir,
            yarn_local_working_dir,
            ENV_VARIABLES["DEPENDENCIES_PACKAGE_NAME"],
            yarn_local_working_dir,
        )
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

    generate_serve_dbt_docs = "cd {}/{} && {}/dbt-venv/bin/dbt docs generate --profiles-dir={}/{} ; echo 'DBT docs hosted on port {} on host: ' $(hostname) >&2 && python3 -m http.server {} --directory target".format(
        yarn_local_working_dir,
        ENV_VARIABLES["DBT_PROJECT_NAME"],
        yarn_local_working_dir,
        yarn_local_working_dir,
        ENV_VARIABLES["DBT_PROJECT_NAME"],
        ENV_VARIABLES["DBT_DOCS_PORT"],
        ENV_VARIABLES["DBT_DOCS_PORT"],
    )

    # commands are meant to sequentially after previous success except dbt_logs_command that runs regardless of dbt_command success/failure.
    launch_command = "{} && {} && {} && {} && {}".format(
        create_working_dir_command,
        download_python_dependencies_from_hdfs,
        populate_working_dir_command,
        download_dbt_project_from_hdfs,
        generate_serve_dbt_docs,
    )

    logging.info(launch_command)

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
        ENV_VARIABLES["YARN_RM_URI"] + "/app/v1/services",
        data=json.dumps(payload),
        auth=gssapi_auth,
        headers=headers,
        verify=False,
    )
    print(response.text)

