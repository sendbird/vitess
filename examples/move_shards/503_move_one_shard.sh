#!/bin/bash

# Copyright 2020 The Vitess Authors.
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

# this script copies the data from customer/0 to customer/-80 and customer/80-
# each row will be copied to exactly one shard based on the vindex value

source ./env.sh

vtctlclient MoveTables -- -source customer --tables 'customer,corder' --source_shards '-80'  Create customer2.partial1

sleep 10

vtctlclient VDiff customer2.partial1

vtctlclient MoveTables SwitchTraffic customer2.partial1