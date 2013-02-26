/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Simple function to return the out degree for each vertex.
 */
@Algorithm(
    name = "Indegree Count"
)
public class SimpleInDegreeCountVertex extends Vertex<
  LongWritable, LongWritable, DoubleWritable, DoubleWritable> {

  @Override
  public void compute(Iterable<DoubleWritable> messages) {
    if (getSuperstep() == 0) {
      Iterable<Edge<LongWritable, DoubleWritable>> edges = getEdges();
      for (Edge<LongWritable, DoubleWritable> edge : edges) {
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(1.0));
      }
    } else {
      long sum = 0;
      for (DoubleWritable message : messages) {
        sum++;
      }
      LongWritable vertexValue = getValue();
      vertexValue.set(sum);
      setValue(vertexValue);
      voteToHalt();
    }
  }
}
