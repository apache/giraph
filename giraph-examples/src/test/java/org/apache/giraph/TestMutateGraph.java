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

package org.apache.giraph;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.SimpleMutateGraphComputation;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexOutputFormat;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexChanges;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for graph mutation
 */
public class TestMutateGraph extends BspCase {
  public TestMutateGraph() {
      super(TestMutateGraph.class.getName());
  }
  /**
   * Custom vertex resolver
   */
  public static class TestVertexResolver<I extends WritableComparable, V
      extends Writable, E extends Writable>
      extends DefaultVertexResolver {
    @Override
    public Vertex resolve(WritableComparable vertexId, Vertex vertex,
        VertexChanges vertexChanges, boolean hasMessages) {
      Vertex originalVertex = vertex;
      // 1. If the vertex exists, first prune the edges
      removeEdges(vertex, vertexChanges);

      // 2. If vertex removal desired, remove the vertex.
      vertex = removeVertexIfDesired(vertex, vertexChanges);

      // If vertex removal happens do not add it back even if it has messages.
      if (originalVertex != null && vertex == null) {
        hasMessages = false;
      }

      // 3. If creation of vertex desired, pick first vertex
      // 4. If vertex doesn't exist, but got messages or added edges, create
      vertex = addVertexIfDesired(vertexId, vertex, vertexChanges, hasMessages);

      // 5. If edge addition, add the edges
      addEdges(vertex, vertexChanges);

      return vertex;
    }
  }

  /**
   * Run a job that tests the various graph mutations that can occur
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testMutateGraph()
          throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleMutateGraphComputation.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
    conf.setWorkerContextClass(
        SimpleMutateGraphComputation.SimpleMutateGraphVertexWorkerContext.class);
    GiraphConstants.USER_PARTITION_COUNT.set(conf, 32);
    conf.setNumComputeThreads(8);
    GiraphConstants.VERTEX_RESOLVER_CLASS.set(conf, TestVertexResolver.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf,
        getTempPath(getCallingMethodName()));
    // Overwrite the number of vertices set in BspCase
    GeneratedVertexReader.READER_VERTICES.set(conf, 400);
    assertTrue(job.run(true));
  }
}
