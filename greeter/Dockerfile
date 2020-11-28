#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# FROM python:3.7-alpine

# RUN mkdir -p /app
# WORKDIR /app

# COPY apache_flink_statefun-snapshot-py3-none-any.whl /app
# RUN pip install apache_flink_statefun-snapshot-py3-none-any.whl

# COPY requirements.txt /app
# RUN pip install -r requirements.txt

# COPY greeter.py /app
# COPY messages_pb2.py /app

# EXPOSE 8000

# CMD ["gunicorn", "-b", "0.0.0.0:8000", "-w 4", "greeter:app"]

# Dockerfile for Python whisk docker action
FROM openwhisk/dockerskeleton

ENV FLASK_PROXY_PORT 8080

# Install our action's Python dependencies
ADD requirements.txt /action/requirements.txt
RUN cd /action; pip install -r requirements.txt

# Ensure source assets are not drawn from the cache 
# after this date
ENV REFRESHED_AT 2016-09-05T13:59:39Z
# Add all source assets
ADD . /action
# Rename our executable Python action
ADD greeter.py /action/exec

# Leave CMD as is for Openwhisk
CMD ["/bin/bash", "-c", "cd actionProxy && python -u actionproxy.py"]

