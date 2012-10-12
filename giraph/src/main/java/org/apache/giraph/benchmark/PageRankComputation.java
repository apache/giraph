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

import org.apache.giraph.graph.MutableVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Shared computation of class Pregel-style PageRank computation for benchmark
 * classes.
 */
public class PageRankComputation {
  /** Number of supersteps */
  public static final String SUPERSTEP_COUNT =
      "PageRankBenchmark.superstepCount";

  /**
   * Do not construct.
   */
  private PageRankComputation() { }

  /**
   * Generic page rank algorithm.
   *
   * @param vertex Vertex to compute on.
   * @param messages Iterator of messages from previous superstep.
   */
  public static void computePageRank(
      MutableVertex<LongWritable, DoubleWritable, DoubleWritable,
      DoubleWritable> vertex, Iterable<DoubleWritable> messages) {
    if (vertex.getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue = new DoubleWritable(
          (0.15f / vertex.getTotalNumVertices()) + 0.85f * sum);
      vertex.setValue(vertexValue);
    }

    if (vertex.getSuperstep() < vertex.getConf().getInt(SUPERSTEP_COUNT, -1)) {
      long edges = vertex.getNumEdges();
      vertex.sendMessageToAllEdges(
          new DoubleWritable(vertex.getValue().get() / edges));
    } else {
      vertex.voteToHalt();
    }
  }
}
