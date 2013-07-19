# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from org.apache.giraph.combiner import DoubleSumMessageCombiner
from org.apache.giraph.edge import ByteArrayEdges
from org.apache.giraph.jython import JythonJob
from org.apache.hadoop.io import IntWritable
from org.apache.hadoop.io import NullWritable


def prepare(job):
    job.hive_database = "default"
    job.workers = 3

    job.computation_name = "CountEdges"

    job.vertex_id.type = IntWritable
    job.vertex_value.type = IntWritable
    job.edge_value.type = NullWritable
    job.message_value.type = NullWritable

    edge_input = JythonJob.EdgeInput()
    edge_input.table = "count_edges_edge_input"
    edge_input.source_id_column = "source_edge_id"
    edge_input.target_id_column = "target_edge_id"
    job.edge_inputs.add(edge_input)

    job.vertex_output.table = "count_edges_output"
    job.vertex_output.id_column = "vertex_id"
    job.vertex_output.value_column = "num_edges"
