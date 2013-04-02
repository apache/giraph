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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Shortest paths algorithm.
 */
public class ShortestPathsVertex extends Vertex<LongWritable, DoubleWritable,
    DoubleWritable, DoubleWritable> {
  /** Source id. */
  public static final String SOURCE_ID =
      "giraph.shortestPathsBenchmark.sourceId";
  /** Default source id. */
  public static final long SOURCE_ID_DEFAULT = 1;

  private boolean isSource() {
    return getId().get() == getConf().getLong(SOURCE_ID, SOURCE_ID_DEFAULT);
  }

  @Override
  public void compute(Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      setValue(new DoubleWritable(Double.MAX_VALUE));
    }

    double minDist = isSource() ? 0d : Double.MAX_VALUE;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }

    if (minDist < getValue().get()) {
      setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
        double distance = minDist + edge.getValue().get();
        sendMessage(edge.getTargetVertexId(),
            new DoubleWritable(distance));
      }
    }

    voteToHalt();
  }
}
