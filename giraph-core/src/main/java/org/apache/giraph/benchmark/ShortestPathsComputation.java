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

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Default single-source shortest paths computation.
 */
public class ShortestPathsComputation {
  /** Source id. */
  public static final String SOURCE_ID = "ShortestPathsBenchmark.sourceId";
  /** Default source id. */
  public static final long SOURCE_ID_DEFAULT = 1;

  /** Do not construct. */
  private ShortestPathsComputation() { };

  /**
   * Is this vertex the source?
   *
   * @param vertex Candidate vertex
   * @return Whether the vertex is the source.
   */
  private static boolean isSource(Vertex<LongWritable, DoubleWritable,
      DoubleWritable, DoubleWritable> vertex) {
    return vertex.getId().get() ==
        vertex.getContext().getConfiguration().getLong(SOURCE_ID,
            SOURCE_ID_DEFAULT);
  }

  /**
   * Generic single-source shortest paths algorithm.
   *
   * @param vertex Vertex to run
   * @param messages Incoming messages for vertex
   */
  public static void computeShortestPaths(
      Vertex<LongWritable, DoubleWritable, DoubleWritable,
          DoubleWritable> vertex,
      Iterable<DoubleWritable> messages) {
    if (vertex.getSuperstep() == 0) {
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
        vertex.sendMessage(edge.getTargetVertexId(),
            new DoubleWritable(distance));
      }
    }

    vertex.voteToHalt();
  }
}
