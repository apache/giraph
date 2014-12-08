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
package org.apache.giraph.debugger.examples.mwm;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * The implementation of an approximate maximum weight matching algorithm.
 * Vertices pick their max-weight neighbors and if two vertices u and v pick
 * each other, they are matched and removed from the graph. Edges
 * pointing to u and v are also removed from the graph.
 */
public class MWMComputation extends
  BasicComputation<LongWritable, VertexValue, DoubleWritable, LongWritable> {

  @Override
  public void compute(Vertex<LongWritable, VertexValue, DoubleWritable> vertex,
    Iterable<LongWritable> messages) throws IOException {
    if (getSuperstep() > 500) {
      vertex.voteToHalt();
    }
    long phase = getSuperstep() % 2;
    if (vertex.getValue().isMatched() || vertex.getNumEdges() == 0) {
      vertex.voteToHalt();
      return;
    }
    if (phase == 0) {
      removeEdges(vertex, messages);
      if (vertex.getNumEdges() == 0) {
        vertex.voteToHalt();
        return;
      }
      long maxValueVertexID = pickMaxValueVertex(vertex);
      vertex.getValue().setMatchedID(maxValueVertexID);
      vertex.setValue(vertex.getValue());
      sendMessage(new LongWritable(maxValueVertexID), vertex.getId());
    } else if (phase == 1) {
      long matchedID = vertex.getValue().getMatchedID();
      boolean isMatched = false;
      for (LongWritable matchingVertexID : messages) {
        if (matchingVertexID.get() == matchedID) {
          isMatched = true;
          break;
        }
      }
      if (isMatched) {
        vertex.getValue().setMatched(true);
        vertex.setValue(vertex.getValue());
        sendMessageToAllEdges(vertex, vertex.getId());
        vertex.voteToHalt();
      }
    }
  }

  /**
   * Remove edges.
   *
   * @param vertex the vertex
   * @param messages the incoming messages
   */
  private void removeEdges(
    Vertex<LongWritable, VertexValue, DoubleWritable> vertex,
    Iterable<LongWritable> messages) {
    for (LongWritable matchedNbr : messages) {
      vertex.removeEdges(matchedNbr);
    }
  }

  /**
   * @param vertex the vertex
   * @return the max id among neighbor vertices
   */
  private long pickMaxValueVertex(
    Vertex<LongWritable, VertexValue, DoubleWritable> vertex) {
    long maxWeightNbrID = -1;
    double maxWeight = -1.0;
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      if (maxWeightNbrID == -1) {
        maxWeightNbrID = edge.getTargetVertexId().get();
        maxWeight = edge.getValue().get();
      } else {
        if (edge.getValue().get() > maxWeight) {
          maxWeightNbrID = edge.getTargetVertexId().get();
          maxWeight = edge.getValue().get();
        }
      }
    }
    return maxWeightNbrID;
  }
}
