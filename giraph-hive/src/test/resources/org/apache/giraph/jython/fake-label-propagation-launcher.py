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

from org.apache.hadoop.io import IntWritable
from org.apache.giraph.jython import JythonJob


def prepare(job):
    job.hive_database = "default"
    job.workers = 5

    job.computation_name = "FakeLabelPropagation"

    job.vertex_id.type = IntWritable

    job.vertex_value.type = FakeLPVertexValue
    job.vertex_value.hive_io = FakeLPVertexValueHive

    job.edge_value.type = FakeLPEdgeValue
    job.edge_value.hive_reader = FakeLPEdgeReader

    job.message_value.type = "FakeLPMessageValue"

    edge_input = JythonJob.EdgeInput()
    edge_input.table = "flp_edges"
    edge_input.source_id_column = "source_id"
    edge_input.target_id_column = "target_id"
    edge_input.value_column = "value"
    job.edge_inputs.add(edge_input)

    vertex_input = JythonJob.VertexInput()
    vertex_input.table = "flp_vertexes"
    vertex_input.id_column = "id"
    vertex_input.value_column = "value"
    job.vertex_inputs.add(vertex_input)

    job.vertex_output.table = "flp_output"
    job.vertex_output.id_column = "id"
    job.vertex_output.value_column = "value"
