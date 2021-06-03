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
package org.apache.giraph.debugger.examples.integrity;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id in the
 * component)
 *
 * The idea behind the algorithm is very simple: propagate the smallest vertex
 * id along the edges to all vertices of a connected component. The number of
 * supersteps necessary is equal to the length of the maximum diameter of all
 * components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 */
public class BuggyConnectedComponentsComputation extends
  BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

  /**
   * Propagates the smallest vertex id to all neighbors. Will always choose to
   * halt and only reactivate if a smaller id has been sent to it.
   *
   * @param vertex
   *          Vertex
   * @param messages
   *          Iterator of messages from the previous superstep.
   * @throws IOException
   */
  @Override
  public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,
    Iterable<LongWritable> messages) throws IOException {
    long currentComponent = vertex.getValue().get();

    if (getSuperstep() == 0) {
      vertex.setValue(vertex.getId());
      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getValue());
      }
      vertex.voteToHalt();
      return;
    }

    boolean changed = false;
    // did we get a smaller id ?
    for (LongWritable message : messages) {
      long candidateComponent = message.get();
      // INTENTIONAL BUG: in the original algorithm the value of the comparison
      // sign should be <.
      // We note that this algorithm will end up finding the components
      // correctly, but the ID of
      // each component will be the minimum vertex ID in the component.
      // In the original
      // org.apache.giraph.examples.ConnectedComponentsComputation, the ID of
      // each
      // component is the maximum vertex ID in the component.
      if (candidateComponent > currentComponent) {
        currentComponent = candidateComponent;
        changed = true;
      }
    }

    // propagate new component id to the neighbors
    if (changed) {
      vertex.setValue(new LongWritable(currentComponent));
      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getValue());
      }
    }
    vertex.voteToHalt();
  }
}
