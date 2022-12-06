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
import re
import os
import sys
import pip


TARGET_PATH = os.environ["TARGET_PATH"]
PORT = int(os.environ["CDSW_APP_PORT"])
#print(TARGET_PATH)

try:
    import flask
except ImportError:
    pip.main(['install', '--user', 'flask'])
    import flask

try:
    import serve
except ImportError:
    pip.main(['install', '--user', 'waitress'])
    import waitress


from flask import Flask, send_from_directory

app = Flask(__name__,
            static_url_path='',
            static_folder= TARGET_PATH)

@app.route('/')
def root():
    return send_from_directory(TARGET_PATH, 'logs/dbt.log')

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="127.0.0.1", port=PORT)
