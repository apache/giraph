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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.WorkerContext;

/**
 * Vertex to allow unit testing of graph mutations.
 */
public class SimpleMutateGraphVertex extends EdgeListVertex<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** Class logger */
  private static Logger LOG =
      Logger.getLogger(SimpleMutateGraphVertex.class);
  /** Maximum number of ranges for vertex ids */
  private long maxRanges = 100;


  /**
   * Unless we create a ridiculous number of vertices , we should not
   * collide within a vertex range defined by this method.
   *
   * @param range Range index
   * @return Starting vertex id of the range
   */
  private long rangeVertexIdStart(int range) {
    return (Long.MAX_VALUE / maxRanges) * range;
  }

  @Override
  public void compute(Iterator<DoubleWritable> msgIterator)
    throws IOException {
    SimpleMutateGraphVertexWorkerContext workerContext =
        (SimpleMutateGraphVertexWorkerContext) getWorkerContext();
    if (getSuperstep() == 0) {
      LOG.debug("Reached superstep " + getSuperstep());
    } else if (getSuperstep() == 1) {
      // Send messages to vertices that are sure not to exist
      // (creating them)
      LongWritable destVertexId =
          new LongWritable(rangeVertexIdStart(1) + getVertexId().get());
      sendMsg(destVertexId, new DoubleWritable(0.0));
    } else if (getSuperstep() == 2) {
      LOG.debug("Reached superstep " + getSuperstep());
    } else if (getSuperstep() == 3) {
      long vertexCount = workerContext.getVertexCount();
      if (vertexCount * 2 != getNumVertices()) {
        throw new IllegalStateException(
            "Impossible to have " + getNumVertices() +
            " vertices when should have " + vertexCount * 2 +
            " on superstep " + getSuperstep());
      }
      long edgeCount = workerContext.getEdgeCount();
      if (edgeCount != getNumEdges()) {
        throw new IllegalStateException(
            "Impossible to have " + getNumEdges() +
            " edges when should have " + edgeCount +
            " on superstep " + getSuperstep());
      }
      // Create vertices that are sure not to exist (doubling vertices)
      LongWritable vertexIndex =
          new LongWritable(rangeVertexIdStart(3) + getVertexId().get());
      BasicVertex<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> vertex =
        instantiateVertex(vertexIndex, null, null, null);
      addVertexRequest(vertex);
      // Add edges to those remote vertices as well
      addEdgeRequest(vertexIndex,
          new Edge<LongWritable, FloatWritable>(
              getVertexId(), new FloatWritable(0.0f)));
    } else if (getSuperstep() == 4) {
      LOG.debug("Reached superstep " + getSuperstep());
    } else if (getSuperstep() == 5) {
      long vertexCount = workerContext.getVertexCount();
      if (vertexCount * 2 != getNumVertices()) {
        throw new IllegalStateException(
            "Impossible to have " + getNumVertices() +
            " when should have " + vertexCount * 2 +
            " on superstep " + getSuperstep());
      }
      long edgeCount = workerContext.getEdgeCount();
      if (edgeCount + vertexCount != getNumEdges()) {
        throw new IllegalStateException(
            "Impossible to have " + getNumEdges() +
            " edges when should have " + edgeCount + vertexCount +
            " on superstep " + getSuperstep());
      }
      // Remove the edges created in superstep 3
      LongWritable vertexIndex =
          new LongWritable(rangeVertexIdStart(3) + getVertexId().get());
      workerContext.increaseEdgesRemoved();
      removeEdgeRequest(vertexIndex, getVertexId());
    } else if (getSuperstep() == 6) {
      // Remove all the vertices created in superstep 3
      if (getVertexId().compareTo(
          new LongWritable(rangeVertexIdStart(3))) >= 0) {
        removeVertexRequest(getVertexId());
      }
    } else if (getSuperstep() == 7) {
      long origEdgeCount = workerContext.getOrigEdgeCount();
      if (origEdgeCount != getNumEdges()) {
        throw new IllegalStateException(
            "Impossible to have " + getNumEdges() +
            " edges when should have " + origEdgeCount +
            " on superstep " + getSuperstep());
      }
    } else if (getSuperstep() == 8) {
      long vertexCount = workerContext.getVertexCount();
      if (vertexCount / 2 != getNumVertices()) {
        throw new IllegalStateException(
            "Impossible to have " + getNumVertices() +
            " vertices when should have " + vertexCount / 2 +
            " on superstep " + getSuperstep());
      }
    } else {
      voteToHalt();
    }
  }

  /**
   * Worker context used with {@link SimpleMutateGraphVertex}.
   */
  public static class SimpleMutateGraphVertexWorkerContext
      extends WorkerContext {
    /** Cached vertex count */
    private long vertexCount;
    /** Cached edge count */
    private long edgeCount;
    /** Original number of edges */
    private long origEdgeCount;
    /** Number of edges removed during superstep */
    private int edgesRemoved = 0;

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException { }

    @Override
    public void postApplication() { }

    @Override
    public void preSuperstep() { }

    @Override
    public void postSuperstep() {
      vertexCount = getNumVertices();
      edgeCount = getNumEdges();
      if (getSuperstep() == 1) {
        origEdgeCount = edgeCount;
      }
      LOG.info("Got " + vertexCount + " vertices, " +
          edgeCount + " edges on superstep " +
          getSuperstep());
      LOG.info("Removed " + edgesRemoved);
      edgesRemoved = 0;
    }

    public long getVertexCount() {
      return vertexCount;
    }

    public long getEdgeCount() {
      return edgeCount;
    }

    public long getOrigEdgeCount() {
      return origEdgeCount;
    }

    /**
     * Increase the number of edges removed by one.
     */
    public void increaseEdgesRemoved() {
      this.edgesRemoved++;
    }
  }
}
