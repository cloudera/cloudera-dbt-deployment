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

if len (sys.argv) != 2 :
    print("Usage: python job-git-clone <project-path> ")
    sys.exit (1)
else:
    DBT_PATH=sys.argv[1]
    if os.path.exists(DBT_PATH):
        os.chdir(DBT_PATH)
        if (subprocess.run(["git","pull"]).returncode !=0 ):
            print("git pull failed")
            sys.exit(1)
    else:
        os.chdir(os.environ.get("DBT_HOME"))
        if (subprocess.run(["git","clone",os.environ.get("DBT_GIT_REPO")]).returncode != 0):
            print("git clone failed")
            sys.exit(1)

