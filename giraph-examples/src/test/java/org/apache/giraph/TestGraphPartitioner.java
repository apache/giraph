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

import static org.apache.giraph.examples.GeneratedVertexReader.READER_VERTICES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.examples.SimpleCheckpoint;
import org.apache.giraph.examples.SimpleSuperstepComputation.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepComputation.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.partition.HashRangePartitionerFactory;
import org.apache.giraph.partition.PartitionBalancer;
import org.apache.giraph.partition.SimpleLongRangePartitionerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Unit test for manual checkpoint restarting
 */
public class TestGraphPartitioner extends BspCase {
  public TestGraphPartitioner() {
    super(TestGraphPartitioner.class.getName());
  }

  private void verifyOutput(FileSystem fs, Path outputPath)
      throws IOException {
    // TODO: this is fragile (breaks with legit serialization changes)
    final int correctLen = 120;
    if (runningInDistributedMode()) {
      FileStatus[] fileStatusArr = fs.listStatus(outputPath);
      int totalLen = 0;
      for (FileStatus fileStatus : fileStatusArr) {
        if (fileStatus.getPath().toString().contains("/part-m-")) {
          totalLen += fileStatus.getLen();
        }
      }
      assertEquals(correctLen, totalLen);
    }
  }

  /**
   * Run a sample BSP job locally and test various partitioners and
   * partition algorithms.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testPartitioners()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath = getTempPath("testVertexBalancer");
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(
        SimpleCheckpoint.SimpleCheckpointComputation.class);
    conf.setWorkerContextClass(
        SimpleCheckpoint.SimpleCheckpointVertexWorkerContext.class);
    conf.setMasterComputeClass(
        SimpleCheckpoint.SimpleCheckpointVertexMasterCompute.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    GiraphJob job = prepareJob("testVertexBalancer", conf, outputPath);

    job.getConfiguration().set(
        PartitionBalancer.PARTITION_BALANCE_ALGORITHM,
        PartitionBalancer.VERTICES_BALANCE_ALGORITHM);

    assertTrue(job.run(true));
    FileSystem hdfs = FileSystem.get(job.getConfiguration());

    conf = new GiraphConfiguration();
    conf.setComputationClass(
        SimpleCheckpoint.SimpleCheckpointComputation.class);
    conf.setWorkerContextClass(
        SimpleCheckpoint.SimpleCheckpointVertexWorkerContext.class);
    conf.setMasterComputeClass(
        SimpleCheckpoint.SimpleCheckpointVertexMasterCompute.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    outputPath = getTempPath("testHashPartitioner");
    job = prepareJob("testHashPartitioner", conf, outputPath);
    assertTrue(job.run(true));
    verifyOutput(hdfs, outputPath);

    job = new GiraphJob("testHashRangePartitioner");
    setupConfiguration(job);
    job.getConfiguration().setComputationClass(
        SimpleCheckpoint.SimpleCheckpointComputation.class);
    job.getConfiguration().setWorkerContextClass(
        SimpleCheckpoint.SimpleCheckpointVertexWorkerContext.class);
    job.getConfiguration().setMasterComputeClass(
        SimpleCheckpoint.SimpleCheckpointVertexMasterCompute.class);
    job.getConfiguration().setVertexInputFormatClass(
        SimpleSuperstepVertexInputFormat.class);
    job.getConfiguration().setVertexOutputFormatClass(
        SimpleSuperstepVertexOutputFormat.class);
    job.getConfiguration().setGraphPartitionerFactoryClass(
        HashRangePartitionerFactory.class);
    outputPath = getTempPath("testHashRangePartitioner");
    removeAndSetOutput(job, outputPath);
    assertTrue(job.run(true));
    verifyOutput(hdfs, outputPath);

    job = new GiraphJob("testSimpleRangePartitioner");
    setupConfiguration(job);
    job.getConfiguration().setComputationClass(
        SimpleCheckpoint.SimpleCheckpointComputation.class);
    job.getConfiguration().setWorkerContextClass(
        SimpleCheckpoint.SimpleCheckpointVertexWorkerContext.class);
    job.getConfiguration().setMasterComputeClass(
        SimpleCheckpoint.SimpleCheckpointVertexMasterCompute.class);
    job.getConfiguration().setVertexInputFormatClass(
        SimpleSuperstepVertexInputFormat.class);
    job.getConfiguration().setVertexOutputFormatClass(
        SimpleSuperstepVertexOutputFormat.class);

    job.getConfiguration().setGraphPartitionerFactoryClass(
        SimpleLongRangePartitionerFactory.class);
    long readerVertices =
        READER_VERTICES.getWithDefault(job.getConfiguration(), -1L);
    job.getConfiguration().setLong(
        GiraphConstants.PARTITION_VERTEX_KEY_SPACE_SIZE, readerVertices);

    outputPath = getTempPath("testSimpleRangePartitioner");
    removeAndSetOutput(job, outputPath);
    assertTrue(job.run(true));
    verifyOutput(hdfs, outputPath);
  }
}
