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

import subprocess
import os
import sys

#Sample usage: 
#python ~/scripts/job-dbt-run-with-vars.py ~/dbt-hive-example/dbt_hive_demo --profiles-dir ~/dbt-hive-example/dbt_hive_demo --vars '{"key1": "value1"}'

if len (sys.argv) < 2 :
    print("Usage: python job-dbt-run.py <project-path> <arg1..argn>")
    sys.exit (1)

else:
    DBT_PATH=sys.argv[1]
    if os.path.exists(DBT_PATH):
        os.chdir(DBT_PATH)
        dbtArgs=['dbt','run']
        for arg  in  sys.argv[2:]:
            dbtArgs = dbtArgs + [arg]
        print("Executing {}".format(dbtArgs))
        if (subprocess.run(dbtArgs).returncode != 0):
            print("{} failed".format(dbtArgs))
            sys.exit(1)
    else:
        print("Path with dbt_project.yml and profiles.yml does not exist")
        sys.exit(1)

