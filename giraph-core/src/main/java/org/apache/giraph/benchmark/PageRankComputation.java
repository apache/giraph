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
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Implementation of PageRank in which vertex ids are ints, page rank values
 * are floats, and graph is unweighted.
 */
public class PageRankComputation extends BasicComputation<IntWritable,
    FloatWritable, NullWritable, FloatWritable> {
  /** Number of supersteps */
  public static final String SUPERSTEP_COUNT =
      "giraph.pageRank.superstepCount";

  @Override
  public void compute(
      Vertex<IntWritable, FloatWritable, NullWritable> vertex,
      Iterable<FloatWritable> messages) throws IOException {
    if (getSuperstep() >= 1) {
      float sum = 0;
      for (FloatWritable message : messages) {
        sum += message.get();
      }
      vertex.getValue().set((0.15f / getTotalNumVertices()) + 0.85f * sum);
    }

    if (getSuperstep() < getConf().getInt(SUPERSTEP_COUNT, 0)) {
      sendMessageToAllEdges(vertex,
          new FloatWritable(vertex.getValue().get() / vertex.getNumEdges()));
    } else {
      vertex.voteToHalt();
    }
  }
}
