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

package org.apache.giraph.benchmark;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Shortest paths algorithm.
 */
public class ShortestPathsComputation extends BasicComputation<LongWritable,
    DoubleWritable, DoubleWritable, DoubleWritable> {
  /** Source id. */
  public static final String SOURCE_ID =
      "giraph.shortestPathsBenchmark.sourceId";
  /** Default source id. */
  public static final long SOURCE_ID_DEFAULT = 1;

  /**
   * Check if vertex is source from which to calculate shortest paths.
   *
   * @param vertex Vertex
   * @return True iff vertex is source for shortest paths
   */
  private boolean isSource(
      Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex) {
    return vertex.getId().get() ==
        getConf().getLong(SOURCE_ID, SOURCE_ID_DEFAULT);
  }

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }

    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }

    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        sendMessage(edge.getTargetVertexId(),
            new DoubleWritable(distance));
      }
    }

    vertex.voteToHalt();
  }
}
