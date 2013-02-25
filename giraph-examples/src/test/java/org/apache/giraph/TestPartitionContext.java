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

import org.apache.giraph.conf.GiraphClasses;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.PartitionContextTestVertex;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestPartitionContext extends BspCase {
  public TestPartitionContext() {
    super(TestPartitionContext.class.getName());
  }

  @Test
  public void testPartitionContext() throws IOException,
      ClassNotFoundException, InterruptedException {
    if (runningInDistributedMode()) {
      System.out.println(
          "testComputeContext: Ignore this test in distributed mode.");
      return;
    }
    GiraphClasses<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
        classes = new GiraphClasses();
    classes.setVertexClass(PartitionContextTestVertex.class);
    classes.setVertexInputFormatClass(
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
    classes.setWorkerContextClass(
        PartitionContextTestVertex.TestPartitionContextWorkerContext.class);
    classes.setPartitionContextClass(
        PartitionContextTestVertex.TestPartitionContextPartitionContext.class);
    GiraphJob job = prepareJob(getCallingMethodName(), classes);
    // Use multithreading
    job.getConfiguration().setNumComputeThreads(
        PartitionContextTestVertex.NUM_COMPUTE_THREADS);
    // Increase the number of vertices
    job.getConfiguration().setInt(
        GeneratedVertexReader.READER_VERTICES,
        PartitionContextTestVertex.NUM_VERTICES);
    // Increase the number of partitions
    job.getConfiguration().setInt(
        GiraphConstants.USER_PARTITION_COUNT,
        PartitionContextTestVertex.NUM_PARTITIONS);
    assertTrue(job.run(true));
  }
}
