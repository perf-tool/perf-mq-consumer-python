#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os

import pulsar

pulsar_host = os.environ.get("PULSAR_HOST", "localhost")
pulsar_port = os.environ.get("PULSAR_PORT", "6650")


def start():
    client = pulsar.Client('pulsar://{}:{}'.format(pulsar_host, pulsar_port))
    consumer = client.subscribe(os.environ.get("PULSAR_TOPIC"), os.environ.get("PULSAR_SUBSCRIPTION_NAME"))
    while True:
        msg = consumer.receive()
        try:
            print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
            consumer.acknowledge(msg)
        except Exception:
            consumer.negative_acknowledge(msg)
