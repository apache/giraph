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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexOutputFormat;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.ooc.CheckMemoryCallable;
import org.apache.giraph.ooc.DiskBackedPartitionStore;
import org.apache.giraph.ooc.MemoryEstimator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for out-of-core mechanism
 */
public class TestOutOfCore extends BspCase {
  final static int NUM_PARTITIONS = 32;
  final static int MAX_NUM_PARTITIONS_IN_MEMORY = 16;
  final static int FAIR_NUM_PARTITIONS_IN_MEMORY = 12;

  public TestOutOfCore() {
      super(TestOutOfCore.class.getName());
  }

  public static class EmptyComputation extends BasicComputation<
      LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    @Override
    public void compute(
        Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
      if (getSuperstep() > 5) {
        vertex.voteToHalt();
      }
    }
  }

  public static class TestMemoryEstimator implements MemoryEstimator {
    private DiskBackedPartitionStore partitionStore;
    @Override
    public void initialize(CentralizedServiceWorker serviceWorker) {
      partitionStore =
          (DiskBackedPartitionStore) serviceWorker.getPartitionStore();
    }

    @Override
    public double freeMemoryMB() {
      int numPartitions = partitionStore.getNumPartitionInMemory();
      if (numPartitions > MAX_NUM_PARTITIONS_IN_MEMORY) {
        return 1;
      } else if (numPartitions > FAIR_NUM_PARTITIONS_IN_MEMORY) {
        return 10;
      } else {
        return 40;
      }
    }

    @Override
    public double maxMemoryMB() {
      return 100;
    }
  }
  /**
   * Run a job that tests the adaptive out-of-core mechanism
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testOutOfCore()
          throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
    GiraphConstants.USER_PARTITION_COUNT.set(conf, NUM_PARTITIONS);
    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);
    GiraphConstants.OUT_OF_CORE_MEM_ESTIMATOR
        .set(conf, TestMemoryEstimator.class);
    CheckMemoryCallable.CHECK_MEMORY_INTERVAL.set(conf, 5);
    GiraphConstants.NUM_COMPUTE_THREADS.set(conf, 8);
    GiraphConstants.NUM_INPUT_THREADS.set(conf, 8);
    GiraphConstants.NUM_OOC_THREADS.set(conf, 4);
    GiraphConstants.NUM_OUTPUT_THREADS.set(conf, 8);
    GiraphJob job = prepareJob(getCallingMethodName(), conf,
        getTempPath(getCallingMethodName()));
    // Overwrite the number of vertices set in BspCase
    GeneratedVertexReader.READER_VERTICES.set(conf, 400);
    assertTrue(job.run(true));
  }
}
