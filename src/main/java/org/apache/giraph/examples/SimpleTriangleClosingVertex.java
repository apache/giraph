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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.examples.SimpleTriangleClosingVertex.IntArrayWritable;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;

/**
 * Demonstrates triangle closing in simple,
 * unweighted graphs for Giraph.
 *
 * Triangle Closing: Vertex A and B maintain out-edges to C and D
 * The algorithm, when finished, populates all vertices' value with an
 * array of Writables representing all the vertices that each
 * should form an out-edge to (connect with, if this is a social
 * graph.)
 * In this example, vertices A and B would hold empty arrays
 * since they are already connected with C and D. Results:
 * If the graph is undirected, C would hold value, D and D would
 * hold value C, since both are neighbors of A and B and yet both
 * were not previously connected to each other.
 *
 * In a social graph, the result values for vertex X would represent people
 * that are likely a part of a person X's social circle (they know one or more
 * people X is connected to already) but X had not previously met them yet.
 * Given this new information, X can decide to connect to vertices (peoople) in
 * the result array or not.
 *
 * Results at each vertex are ordered in terms of the # of neighbors
 * who are connected to each vertex listed in the final vertex value.
 * The more of a vertex's neighbors who "know" someone, the stronger
 * your social relationship is presumed to be to that vertex (assuming
 * a social graph) and the more likely you should connect with them.
 *
 * In this implementation, Edge Values are not used, but could be
 * adapted to represent additional qualities that could affect the
 * ordering of the final result array.
 */
public class SimpleTriangleClosingVertex extends EdgeListVertex<
  IntWritable, SimpleTriangleClosingVertex.IntArrayWritable,
  NullWritable, IntWritable> {
  /** Vertices to close the triangle, ranked by frequency of in-msgs */
  private Map<IntWritable, Integer> closeMap =
    new TreeMap<IntWritable, Integer>();
  /** Set of vertices you already recieved @ least once */
  private Set<Integer> recvSet = new HashSet<Integer>();

  @Override
  public void compute(Iterator<IntWritable> msgIterator) {
    if (getSuperstep() == 0) {
      // obtain list of all out-edges from THIS vertex
      Iterator<IntWritable> iterator = iterator();
      while (iterator.hasNext()) {
        sendMsgToAllEdges(iterator.next());
      }
    } else {
      while (msgIterator.hasNext()) {
        IntWritable iw = msgIterator.next();
        int inId = iw.get();
        if (recvSet.contains(inId)) {
          int current = closeMap.get(iw) == null ? 0 : inId;
          closeMap.put(iw, current + 1);
        }
        if (inId != getVertexId().get()) {
          recvSet.add(inId);
        }
      }
      int ndx = closeMap.size();
      IntWritable[] temp = new IntWritable[ndx];
      for (IntWritable w: closeMap.keySet()) {
        temp[--ndx] = w;
      }
      IntArrayWritable result = new IntArrayWritable();
      result.set(temp);
      setVertexValue(result);
    }
    voteToHalt();
  }

  /** Utility class for delivering the array of vertices THIS vertex
    * should connect with to close triangles with neighbors */
  public static class IntArrayWritable extends ArrayWritable {
    /** default constructor */
    public IntArrayWritable() {
      super(IntWritable.class);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      Writable[] iwArray = get();
      for (int i = 0; i < iwArray.length; ++i) {
        IntWritable iw = (IntWritable) iwArray[i];
        sb.append(iw.get() + "  ");
      }
      return sb.toString();
    }
  }
}
