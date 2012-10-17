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

import java.io.IOException;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.junit.Test;

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
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimplePageRankVertex.class, SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
    job.getConfiguration().setWorkerContextClass(
        SimplePageRankVertex.SimplePageRankVertexWorkerContext.class);
    job.getConfiguration().setMasterComputeClass(
        SimplePageRankVertex.SimplePageRankVertexMasterCompute.class);
    job.getConfiguration().setNumComputeThreads(numComputeThreads);
    // Set enough partitions to generate randomness on the compute side
    if (numComputeThreads != 1) {
      job.getConfiguration().setInt(
          HashMasterPartitioner.USER_PARTITION_COUNT,
          numComputeThreads * 5);
    }
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
      assertEquals(5l, numVertices);
    }
  }
}
