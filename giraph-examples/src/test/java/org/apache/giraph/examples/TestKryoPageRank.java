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
 * Test page rank with kryo wrapper
 */
public class TestKryoPageRank extends BspCase {

  /**
   * Constructor
   */
  public TestKryoPageRank() {
    super(TestPageRank.class.getName());
  }

  @Test
  public void testKryoPageRank()
          throws ClassNotFoundException, IOException, InterruptedException {
    testPageRankWithKryoWrapper(1);
  }

  @Test
  public void testKryoPageRankTenThreadsCompute()
          throws ClassNotFoundException, IOException, InterruptedException {
    testPageRankWithKryoWrapper(10);
  }


  /**
   * Testing simple page rank by wrapping vertex value, edge
   * and message values with kryo wrapper.
   *
   * @param numComputeThreads Number of compute threads to use
   * @throws java.io.IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void testPageRankWithKryoWrapper(int numComputeThreads)
          throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(PageRankWithKryoSimpleWritable.class);
    conf.setVertexInputFormatClass(
            PageRankWithKryoSimpleWritable.PageRankWithKryoVertexInputFormat.class);
    conf.setWorkerContextClass(
            PageRankWithKryoSimpleWritable.PageRankWithKryoWorkerContext.class);
    conf.setMasterComputeClass(
            PageRankWithKryoSimpleWritable.PageRankWithKryoMasterCompute.class);
    conf.setNumComputeThreads(numComputeThreads);
    // Set enough partitions to generate randomness on the compute side
    if (numComputeThreads != 1) {
      GiraphConstants.USER_PARTITION_COUNT.set(conf, numComputeThreads * 5);
    }
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      double maxPageRank =
              PageRankWithKryoSimpleWritable.PageRankWithKryoWorkerContext.getFinalMax();
      double minPageRank =
              PageRankWithKryoSimpleWritable.PageRankWithKryoWorkerContext.getFinalMin();
      long numVertices =
              PageRankWithKryoSimpleWritable.PageRankWithKryoWorkerContext.getFinalSum();
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
