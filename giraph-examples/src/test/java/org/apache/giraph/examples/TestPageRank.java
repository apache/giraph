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

import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test page rank (with and without multithreading)
 */
public class TestPageRank extends BspCase {

  /**
   * Constructor
   */
  public TestPageRank() {
    super(TestPageRank.class.getName());
  }

  @Test
  public void testBspPageRankSingleCompute()
      throws ClassNotFoundException, IOException, InterruptedException {
    testPageRank(1);
  }


  @Test
  public void testPageRankTenThreadsCompute()
      throws ClassNotFoundException, IOException, InterruptedException {
    testPageRank(10);
  }

  /**
   * Generic page rank test
   *
   * @param numComputeThreads Number of compute threads to use
   * @throws java.io.IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void testPageRank(int numComputeThreads)
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexClass(SimplePageRankVertex.class);
    conf.setVertexInputFormatClass(
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
    conf.setWorkerContextClass(
        SimplePageRankVertex.SimplePageRankVertexWorkerContext.class);
    conf.setMasterComputeClass(
        SimplePageRankVertex.SimplePageRankVertexMasterCompute.class);
    conf.setNumComputeThreads(numComputeThreads);
    // Set enough partitions to generate randomness on the compute side
    if (numComputeThreads != 1) {
      GiraphConstants.USER_PARTITION_COUNT.set(conf, numComputeThreads * 5);
    }
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      double maxPageRank =
          SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalMax();
      double minPageRank =
          SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalMin();
      long numVertices =
          SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalSum();
      System.out.println(getCallingMethodName() + ": maxPageRank=" +
          maxPageRank + " minPageRank=" +
          minPageRank + " numVertices=" + numVertices + ", " +
          " numComputeThreads=" + numComputeThreads);
      assertEquals(34.03, maxPageRank, 0.001);
      assertEquals(0.03, minPageRank, 0.00001);
      assertEquals(5L, numVertices);
    }
  }
}
