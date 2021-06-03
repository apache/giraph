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

import org.apache.giraph.debugger.instrumenter.AbstractInterceptingComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * WARNING: This class is should be used only for development. It is put in the
 * Graft source tree to demonstrate to users the two classes that Graft
 * generates at runtime when instrumenting a {@link Computation} class. This is
 * the example for {@link BuggyConnectedComponentsComputation}. The other class
 * Graft generates is {@link BuggyConnectedComponentsDebugComputationToRun}.
 * Please see the Graft documentation for more details on how Graft instruments
 * {@link Computation} classes.
 *
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
public abstract class BuggyConnectedComponentsDebugComputationModified
  extends AbstractInterceptingComputation<IntWritable, IntWritable,
  NullWritable, IntWritable, IntWritable> {

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
  public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
    Iterable<IntWritable> messages) throws IOException {
    int currentComponent = vertex.getValue().get();

    if (getSuperstep() == 0) {
      vertex.setValue(new IntWritable(currentComponent));
      for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getValue());
      }
      vertex.voteToHalt();
      return;
    }

    boolean changed = false;
    // did we get a smaller id ?
    for (IntWritable message : messages) {
      int candidateComponent = message.get();
      // INTENTIONAL BUG: in the original algorithm the value of the comparison
      // sign should be <.
      if (candidateComponent > currentComponent) {
        System.out.print("changing value in superstep: " + getSuperstep() +
          " vertex.id: " + vertex.getId() + " newComponent: " +
          candidateComponent + "\n");
        currentComponent = candidateComponent;
        changed = true;
      }
    }

    // propagate new component id to the neighbors
    if (changed) {
      vertex.setValue(new IntWritable(currentComponent));
      for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getValue());
      }
    }
    vertex.voteToHalt();
  }
}
