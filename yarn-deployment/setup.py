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

from setuptools import find_packages, find_namespace_packages, setup

import re
import os

# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), "r", encoding="utf8") as f:
    long_description = f.read()

package_name = "cloudera-dbt-deployment"
package_version = "1.1.2"
description = """Package to install dbt and required libraries in cloudera environment"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Cloudera",
    author_email="innovation-feedback@cloudera.com",
    url="https://github.com/cloudera/cloudera-dbt-deployment",
    data_files=[('', ['profiles.yml','dbt-docs.json'])],
    include_package_data=False,
    setup_requires=[
        "setuptools",
    ],
    install_requires=[
        "python-dotenv",       
        "dbt-hive",
        "dbt-impala",
        "dbt-spark-livy",
    ],
    python_requires=">=3.7.2",
    scripts=['dbt_commands.py'],
)
