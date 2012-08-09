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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.giraph.benchmark.PageRankBenchmark;
import org.apache.giraph.benchmark.PageRankComputation;
import org.apache.giraph.io.PseudoRandomVertexInputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.io.JsonBase64VertexInputFormat;
import org.apache.giraph.io.JsonBase64VertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Test;

/**
 * Test out the JsonBase64 format.
 */
public class TestJsonBase64Format extends BspCase {
  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public TestJsonBase64Format(String testName) {
    super(testName);
  }

  public TestJsonBase64Format() {
    super(TestJsonBase64Format.class.getName());
  }

  /**
   * Start a job and finish after i supersteps, then begin a new job and
   * continue on more j supersteps.  Check the results against a single job
   * with i + j supersteps.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testContinue()
      throws IOException, InterruptedException, ClassNotFoundException {

    Path outputPath = getTempPath(getCallingMethodName());
    GiraphJob job = prepareJob(getCallingMethodName(), PageRankBenchmark.class,
        PseudoRandomVertexInputFormat.class,
        JsonBase64VertexOutputFormat.class, outputPath);
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.AGGREGATE_VERTICES, 101);
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.EDGES_PER_VERTEX, 2);
    job.getConfiguration().setInt(PageRankComputation.SUPERSTEP_COUNT, 2);

    assertTrue(job.run(true));

    Path outputPath2 = getTempPath(getCallingMethodName() + "2");
    job = prepareJob(getCallingMethodName(), PageRankBenchmark.class,
        JsonBase64VertexInputFormat.class, JsonBase64VertexOutputFormat.class,
        outputPath2);
    job.getConfiguration().setInt(PageRankComputation.SUPERSTEP_COUNT, 3);
    FileInputFormat.setInputPaths(job.getInternalJob(), outputPath);
    assertTrue(job.run(true));

    Path outputPath3 = getTempPath(getCallingMethodName() + "3");
    job = prepareJob(getCallingMethodName(), PageRankBenchmark.class,
        PseudoRandomVertexInputFormat.class,
        JsonBase64VertexOutputFormat.class, outputPath3);
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.AGGREGATE_VERTICES, 101);
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.EDGES_PER_VERTEX, 2);
    job.getConfiguration().setInt(PageRankComputation.SUPERSTEP_COUNT, 5);
    assertTrue(job.run(true));

    Configuration conf = job.getConfiguration();

    assertEquals(101, getNumResults(conf, outputPath));
    assertEquals(101, getNumResults(conf, outputPath2));
    assertEquals(101, getNumResults(conf, outputPath3));
  }
}
