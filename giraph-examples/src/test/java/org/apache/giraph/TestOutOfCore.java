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
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for out-of-core mechanism
 */
public class TestOutOfCore extends BspCase {
  final static int NUM_PARTITIONS = 32;
  final static int NUM_PARTITIONS_IN_MEMORY = 16;

  public TestOutOfCore() {
      super(TestOutOfCore.class.getName());
  }

  /**
   * Run a job that tests the fixed out-of-core mechanism
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testOutOfCore()
          throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
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
    GiraphConstants.NUM_COMPUTE_THREADS.set(conf, 8);
    GiraphConstants.NUM_INPUT_THREADS.set(conf, 8);
    GiraphConstants.NUM_OOC_THREADS.set(conf, 4);
    GiraphConstants.NUM_OUTPUT_THREADS.set(conf, 8);
    GiraphConstants.PARTITIONS_DIRECTORY.set(conf, "disk0,disk1,disk2");
    GiraphJob job = prepareJob(getCallingMethodName(), conf,
        getTempPath(getCallingMethodName()));
    // Overwrite the number of vertices set in BspCase
    GeneratedVertexReader.READER_VERTICES.set(conf, 200);
    assertTrue(job.run(true));
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
