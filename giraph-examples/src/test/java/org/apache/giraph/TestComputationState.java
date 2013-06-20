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
import org.apache.giraph.examples.TestComputationStateComputation;
import org.apache.giraph.job.GiraphJob;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestComputationState extends BspCase {
  public TestComputationState() {
    super(TestComputationState.class.getName());
  }

  @Test
  public void testComputationState() throws IOException,
      ClassNotFoundException, InterruptedException {
    if (runningInDistributedMode()) {
      System.out.println(
          "testComputeContext: Ignore this test in distributed mode.");
      return;
    }
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(TestComputationStateComputation.class);
    conf.setVertexInputFormatClass(
        SimplePageRankComputation.SimplePageRankVertexInputFormat.class);
    conf.setWorkerContextClass(
        TestComputationStateComputation.TestComputationStateWorkerContext.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    // Use multithreading
    job.getConfiguration().setNumComputeThreads(
        TestComputationStateComputation.NUM_COMPUTE_THREADS);
    // Increase the number of vertices
    GeneratedVertexReader.READER_VERTICES.set(job.getConfiguration(),
        TestComputationStateComputation.NUM_VERTICES);
    // Increase the number of partitions
    GiraphConstants.USER_PARTITION_COUNT.set(job.getConfiguration(),
        TestComputationStateComputation.NUM_PARTITIONS);
    assertTrue(job.run(true));
  }
}
