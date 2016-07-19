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
import org.apache.giraph.examples.SimplePageRankComputation;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexOutputFormat;

import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.ooc.OutOfCoreIOScheduler;
import org.apache.giraph.ooc.persistence.InMemoryDataAccessor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for out-of-core mechanism
 */
public class TestOutOfCore extends BspCase {
  private final static int NUM_PARTITIONS = 400;
  private final static int NUM_PARTITIONS_IN_MEMORY = 8;
  private GiraphConfiguration conf;

  public TestOutOfCore() {
      super(TestOutOfCore.class.getName());
  }

  @Before
  public void prepareTest() {
    conf = new GiraphConfiguration();
    conf.setComputationClass(SimplePageRankComputation.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
    conf.setWorkerContextClass(
        SimplePageRankComputation.SimplePageRankWorkerContext.class);
    conf.setMasterComputeClass(
        SimplePageRankComputation.SimplePageRankMasterCompute.class);
    GiraphConstants.USER_PARTITION_COUNT.set(conf, NUM_PARTITIONS);
    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, NUM_PARTITIONS_IN_MEMORY);
    OutOfCoreIOScheduler.OOC_WAIT_INTERVAL.set(conf, 10);
    GiraphConstants.NUM_COMPUTE_THREADS.set(conf, 8);
    GiraphConstants.NUM_INPUT_THREADS.set(conf, 8);
    GiraphConstants.NUM_OUTPUT_THREADS.set(conf, 8);
  }

  @Test
  public void testOutOfCoreInMemoryAccessor()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConstants.OUT_OF_CORE_DATA_ACCESSOR.set(conf, InMemoryDataAccessor.class);
    GiraphConstants.NUM_OUT_OF_CORE_THREADS.set(conf, 8);
    runTest();
  }

  @Test
  public void testOutOfCoreLocalDiskAccessor()
    throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConstants.PARTITIONS_DIRECTORY.set(conf, "disk0,disk1,disk2");
    runTest();
  }

  /**
   * Run a job with fixed out-of-core policy and verify the result
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private void runTest()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphJob job = prepareJob(getCallingMethodName(), conf,
        getTempPath(getCallingMethodName()));
    // Overwrite the number of vertices set in BspCase
    GeneratedVertexReader.READER_VERTICES.set(conf, 200);
    assertTrue(job.run(false));
    if (!runningInDistributedMode()) {
      double maxPageRank =
          SimplePageRankComputation.SimplePageRankWorkerContext.getFinalMax();
      double minPageRank =
          SimplePageRankComputation.SimplePageRankWorkerContext.getFinalMin();
      long numVertices =
          SimplePageRankComputation.SimplePageRankWorkerContext.getFinalSum();
      System.out.println(getCallingMethodName() + ": maxPageRank=" +
          maxPageRank + " minPageRank=" +
          minPageRank + " numVertices=" + numVertices);
      assertEquals(13591.5, maxPageRank, 0.01);
      assertEquals(9.375e-5, minPageRank, 0.000000001);
      assertEquals(8 * 200L, numVertices);
    }
  }
}
