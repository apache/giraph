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

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class SimpleShortestPathsVertex extends
    EdgeListVertex<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> {
  /** The shortest paths id */
  public static final String SOURCE_ID = "SimpleShortestPathsVertex.sourceId";
  /** Default shortest paths id */
  public static final long SOURCE_ID_DEFAULT = 1;
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleShortestPathsVertex.class);

  /**
   * Is this vertex the source id?
   *
   * @return True if the source id
   */
  private boolean isSource() {
    return getVertexId().get() ==
        getContext().getConfiguration().getLong(SOURCE_ID,
            SOURCE_ID_DEFAULT);
  }

  @Override
  public void compute(Iterator<DoubleWritable> msgIterator) {
    if (getSuperstep() == 0) {
      setVertexValue(new DoubleWritable(Double.MAX_VALUE));
    }
    double minDist = isSource() ? 0d : Double.MAX_VALUE;
    while (msgIterator.hasNext()) {
      minDist = Math.min(minDist, msgIterator.next().get());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + getVertexId() + " got minDist = " + minDist +
          " vertex value = " + getVertexValue());
    }
    if (minDist < getVertexValue().get()) {
      setVertexValue(new DoubleWritable(minDist));
      for (LongWritable targetVertexId : this) {
        FloatWritable edgeValue = getEdgeValue(targetVertexId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + getVertexId() + " sent to " +
              targetVertexId + " = " +
              (minDist + edgeValue.get()));
        }
        sendMsg(targetVertexId,
            new DoubleWritable(minDist + edgeValue.get()));
      }
    }
    voteToHalt();
  }
}
