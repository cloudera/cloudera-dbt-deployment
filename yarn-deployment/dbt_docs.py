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

from datetime import datetime
from dotenv import load_dotenv,find_dotenv
from requests_gssapi import HTTPSPNEGOAuth

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s: %(message)s"
)

# Global dictionary to store required environment variables.
ENV_VARIABLES = {}

# load the environment variables from .env file
def load_fetch_environment_variables():
    logging.info("Loading environment variables.")
    load_dotenv(find_dotenv())
    logging.info("Done Loading environment variables.")

    # fetch the environment variables
    ENV_VARIABLES["dbt_profiles_path"] = os.getenv("DBT_PROFILES_PATH")
    ENV_VARIABLES["yarn_principal"] = os.getenv("YARN_PRINCIPAL")
    ENV_VARIABLES["yarn_keytab"] = os.getenv("YARN_KEYTAB")
    ENV_VARIABLES["yarn_rm_uri"] = os.getenv("YARN_RM_URI")
    ENV_VARIABLES["git_project_url"] = os.getenv("GIT_PROJECT_URL")
    ENV_VARIABLES["dbt_project_path"] = os.getenv("DBT_PROJECT_PATH")


# copy dbt profile file to hdfs
def copy_file_to_hdfs():
    profile_file_path = "--directory={}".format(ENV_VARIABLES["dbt_profiles_path"])
    logging.info(
        "Compressing and Uploading profiles file to hdfs: %s", profile_file_path
    )

    compressed_project_directory = "/tmp/dbt_profiles.tar.gz"
    subprocess.run(
        ["tar", "-zcf", compressed_project_directory, profile_file_path, "profiles.yml"],
        check=True,
        capture_output=True,
        text=True,
    )

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
    logging.info("Done Uploading file to hdfs.")


# generate JSON payload dynamically to send to yarn container to generate /serve dbt docs
def generate_yarn_payload():

    kerberos_principal = {}
    kerberos_principal["principal_name"] = "{}".format(ENV_VARIABLES["yarn_principal"])
    kerberos_principal["keytab"] = "file://{}".format(ENV_VARIABLES["yarn_keytab"])

    copy_file_to_hdfs()

    artifact = {}
    artifact["type"] = "TARBALL"
    artifact["id"] = "hdfs:///tmp/dbt_profiles.tar.gz"

    component = {}
    component["name"] = "dbtdocs"
    component["number_of_containers"] = 1

    yarn_local_profile_dir = "/tmp/dbt-{}".format(
        datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    )
    
    component[
        "launch_command"
    ] =  "mkdir -p {} && hdfs dfs -copyToLocal /tmp/dbt_profiles.tar.gz {} && cd {}  && tar -xzvf dbt_profiles.tar.gz && python3 -m venv /tmp/dbt-yarn && source /tmp/dbt-yarn/bin/activate && pip install dbt-hive dbt-impala && git clone $GIT_PROJECT_URL && cd $DBT_PROJECT_PATH && dbt docs generate --profiles-dir={} ; echo 'DBT docs hosted on port 7777 on host: ' $(hostname) >&2 && python3 -m http.server 7777 --directory target ".format(
        yarn_local_profile_dir,
        yarn_local_profile_dir,
        yarn_local_profile_dir,
        yarn_local_profile_dir,
    )
    component["resource"] = {"cpus": 1, "memory": "512"}

    env = {}
    env["GIT_PROJECT_URL"] = ENV_VARIABLES["git_project_url"]
    env["DBT_PROJECT_PATH"] = ENV_VARIABLES["dbt_project_path"] 

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
    load_fetch_environment_variables()
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


def main():
   host_dbt_docs()

