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

import logging
import os
import subprocess
import sys
import uuid

from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s: %(message)s"
)

# Global dictionary to store all environment variables.
ENV_VARIABLES = {}

if len(sys.argv) < 2:
    logging.error("Insufficient arguments to continue execution of script.")
    sys.exit(1)


def load_fetch_environment_variables():
    # load the environment variables from .env file
    logging.info("Loading environment variables.")
    load_dotenv()
    logging.info("Done Loading environment variables.")

    # fetch the environment variables
    ENV_VARIABLES["dbt_profiles_path"] = os.getenv("DBT_PROFILES_PATH")
    ENV_VARIABLES["yarn_jar"] = os.getenv("YARN_JAR")
    ENV_VARIABLES["dbt_user"] = os.getenv("DBT_USER")
    ENV_VARIABLES["dbt_path"] = os.getenv("DBT_PATH")
    ENV_VARIABLES["dbt_user_keytab"] = os.getenv("DBT_USER_KEYTAB")
    ENV_VARIABLES["dbt_principal"] = os.getenv("DBT_PRINCIPAL")
    ENV_VARIABLES["git_url"] = os.getenv("GIT_URL")
    ENV_VARIABLES["git_project_name"] = os.getenv("GIT_PROJECT_NAME")
    ENV_VARIABLES["dbt_adapter_type"] = os.getenv("DBT_ADAPTER_TYPE")


def generate_yarn_shell_command():

    working_dir = "{}/dbt-{}".format(
        ENV_VARIABLES["dbt_path"], datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    )

    kinit_command = "kinit -kt {} {}".format(
        ENV_VARIABLES["dbt_user_keytab"], ENV_VARIABLES["dbt_principal"]
    )

    create_working_dir_command = "mkdir -p {}/profile-dir && cp profiles.yml {}/profile-dir && python3 -m venv {}/dbt-venv".format(
        working_dir,
        working_dir,
        working_dir,
    )

    populate_working_dir_command = "source {}/dbt-venv/bin/activate".format(
        working_dir,
    )

    git_command = "git clone {} && cd {}".format(
        ENV_VARIABLES["git_url"],
        ENV_VARIABLES["git_project_name"],
    )

    dbt_command = "{}/dbt-venv/bin/dbt {} --profiles-dir={}/profile-dir".format(
        working_dir,
        sys.argv[1],
        working_dir,
    )

    dbt_logs_command = "cat logs/dbt.log >&2"

    # commands are meant to sequentially after previous success except dbt_logs_command that runs regardless of dbt_command success/failure.
    shell_command = "{} && {} && {} && {} && {} ; {}".format(
        kinit_command,
        create_working_dir_command,
        populate_working_dir_command,
        git_command,
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


def launch_yarn_container_with_dbt_command():
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
                "profiles.yml",
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


# load and fetch the environment variables
load_fetch_environment_variables()
launch_yarn_container_with_dbt_command()
