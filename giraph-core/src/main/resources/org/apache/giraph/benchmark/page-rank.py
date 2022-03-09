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

from org.apache.giraph.graph import BasicComputation
from org.apache.hadoop.io import FloatWritable

class PageRank(BasicComputation):
  SUPERSTEP_COUNT = "giraph.pageRank.superstepCount"

  def compute(self, vertex, messages):
    if self.getSuperstep() >= 1:
      total = 0
      for msg in messages:
        total += msg.get()
      vertex.getValue().set((0.15 / self.getTotalNumVertices()) + 0.85 * total)

    if self.getSuperstep() < self.getConf().getInt(self.SUPERSTEP_COUNT, 0):
      self.sendMessageToAllEdges(vertex,
          FloatWritable(vertex.getValue().get() / vertex.getNumEdges()))
    else:
      vertex.voteToHalt()
