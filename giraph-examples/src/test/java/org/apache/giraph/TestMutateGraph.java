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
import org.apache.giraph.examples.SimpleMutateGraphVertex;
import org.apache.giraph.examples.SimplePageRankVertex.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimplePageRankVertex.SimplePageRankVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
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
    conf.setVertexClass(SimpleMutateGraphVertex.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
    conf.setWorkerContextClass(
        SimpleMutateGraphVertex.SimpleMutateGraphVertexWorkerContext.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf,
        getTempPath(getCallingMethodName()));
    assertTrue(job.run(true));
  }
}
