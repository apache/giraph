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
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Implementation of Page Rank algorithm on a weighted graph.
 */
public class WeightedPageRankComputation extends BasicComputation<LongWritable,
    DoubleWritable, DoubleWritable, DoubleWritable> {
  /** Number of supersteps */
  public static final String SUPERSTEP_COUNT =
      "giraph.weightedPageRank.superstepCount";

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      // Normalize out edge weights
      double outEdgeSum = 0;
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        outEdgeSum += edge.getValue().get();
      }
      for (MutableEdge<LongWritable, DoubleWritable> edge :
          vertex.getMutableEdges()) {
        edge.setValue(new DoubleWritable(edge.getValue().get() / outEdgeSum));
      }
    } else {
      double messageSum = 0;
      for (DoubleWritable message : messages) {
        messageSum += message.get();
      }
      vertex.getValue().set(
          (0.15f / getTotalNumVertices()) + 0.85f * messageSum);
    }

    if (getSuperstep() < getConf().getInt(SUPERSTEP_COUNT, 0)) {
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(),
            new DoubleWritable(
                vertex.getValue().get() * edge.getValue().get()));
      }
    } else {
      vertex.voteToHalt();
    }
  }
}
