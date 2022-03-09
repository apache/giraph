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
package org.apache.giraph.debugger.examples.instrumented;

import java.io.IOException;

import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.debugger.examples.simpledebug.SimpleShortestPathsMaster;
import org.apache.giraph.debugger.instrumenter.AbstractInterceptingComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * WARNING: This class is should be used only for development. It is put in the
 * Graft source tree to demonstrate to users the two classes that Graft
 * generates at runtime when instrumenting a {@link Computation} class. This is
 * the example for {@link BuggySimpleShortestPathsComputation}. The other class
 * Graft generates is {@link BuggySimpleShortestPathsDebugComputationToRun}.
 * Please see the Graft documentation for more details on how Graft instruments
 * {@link Computation} classes.
 *
 * Debug version of SimpleShortestPathsComputation.
 */
@Algorithm(name = "Shortest paths", description = "Finds all shortest paths" +
    "from a selected vertex")
public abstract class BuggySimpleShortestPathsDebugComputationModified
  extends AbstractInterceptingComputation<LongWritable, DoubleWritable,
  FloatWritable, DoubleWritable, DoubleWritable> {

  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID = new LongConfOption(
    "SimpleShortestPathsVertex.sourceId", 1, "The shortest paths id");
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(BuggySimpleShortestPathsDebugComputationModified.class);

  /**
   * Is this vertex the source id?
   *
   * @param vertex
   *          Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
    Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
    Iterable<DoubleWritable> messages) throws IOException {
    // We do a dummy read of the aggregator below because for now we only
    // intercept an aggregator
    // if at least one vertex reads it.
    LongWritable aggregatedValue = getAggregatedValue(
      SimpleShortestPathsMaster.NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR);
    if (aggregatedValue != null) {
      System.out.print("NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR: " +
        aggregatedValue.get() + "\n");
    }
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(isSource(vertex) ? 0d :
        Double.MAX_VALUE));
    }
    double previousValue = vertex.getValue().get();
    double minDist = previousValue;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
        " vertex value = " + vertex.getValue());
    }
    if (minDist < vertex.getValue().get() || getSuperstep() == 0 &&
      minDist == 0) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
            edge.getTargetVertexId() + " = " + distance);
        }
        // INTENTIONAL BUG:Instead of sending the distance (i.e. by adding edge
        // values),
        // we send the vertex value.
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(minDist));
      }
    }
    if (previousValue > 3 && minDist <= 3) {
      aggregate(
        SimpleShortestPathsMaster.NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR,
        new LongWritable(1));
    }
    vertex.voteToHalt();
  }
}
