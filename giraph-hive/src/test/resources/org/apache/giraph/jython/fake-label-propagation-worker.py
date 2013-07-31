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

from org.apache.hadoop.io import Writable
from org.apache.giraph.jython import JythonComputation
from org.apache.giraph.hive.jython import JythonHiveIO
from org.apache.giraph.hive.jython import JythonHiveReader


# Implements HiveColumnIO to tell Giraph how to read/write from Hive
class FakeLPVertexValue:
    def __init__(self):
        self.labels = {}
        self.dog = 'cat'

    def add(self, message):
        for label, weight in message.labels.iteritems():
            if label in self.labels:
                self.labels[label] += weight
            else:
                self.labels[label] = weight


# Hive reader/writer for vertexes
class FakeLPVertexValueHive(JythonHiveIO):
    def readFromHive(self, vertex_value, column):
        vertex_value.labels = column.getMap()

    def writeToHive(self, vertex_value, column):
        column.setMap(vertex_value.labels)


# Implements Writable to override default Jython serialization which grabs all
# of the data in an object.
# Also implements HiveColumnReadable to read from Hive.
class FakeLPEdgeValue(Writable):
    def __init__(self):
        self.value = 2.13
        self.foo = "bar"

    def readFields(self, data_input):
        self.value = data_input.readFloat()
        self.foo = "read_in"

    def write(self, data_output):
        data_output.writeFloat(self.value)
        self.foo = "wrote_out"


# Hive reader for edges
class FakeLPEdgeReader(JythonHiveReader):
    def readFromHive(self, edge_value, column):
        edge_value.value = column.getFloat()


# Doesn't implement anything. Use default Jython serialization.
# No need for I/O with Hive
class FakeLPMessageValue:
    def __init__(self):
        self.labels = {}


# Implements BasicComputation to be a Computation Giraph can use
class FakeLabelPropagation(JythonComputation):
    def compute(self, vertex, messages):
        if self.superstep == 0:
            self.send_msg(vertex)
        elif self.superstep < self.conf.getInt("supersteps", 3):
            for message in messages:
                vertex.value.add(message)
            self.send_msg(vertex)
        else:
            vertex.voteToHalt()

    def send_msg(self, vertex):
        msg = FakeLPMessageValue()
        msg.labels = vertex.value.labels
        self.sendMessageToAllEdges(vertex, msg)
