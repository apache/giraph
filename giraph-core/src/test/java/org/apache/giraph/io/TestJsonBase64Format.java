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

package org.apache.giraph.io;

import org.apache.giraph.BspCase;
import org.apache.giraph.benchmark.WeightedPageRankComputation;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.JsonBase64VertexInputFormat;
import org.apache.giraph.io.formats.JsonBase64VertexOutputFormat;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.PseudoRandomVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test out the JsonBase64 format.
 */
public class TestJsonBase64Format extends BspCase {
  /**
   * Constructor.
   */
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
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(WeightedPageRankComputation.class);
    conf.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
    conf.setVertexOutputFormatClass(JsonBase64VertexOutputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf, outputPath);
    job.getConfiguration().setLong(
        PseudoRandomInputFormatConstants.AGGREGATE_VERTICES, 101);
    job.getConfiguration().setLong(
        PseudoRandomInputFormatConstants.EDGES_PER_VERTEX, 2);
    job.getConfiguration().setInt(
        WeightedPageRankComputation.SUPERSTEP_COUNT, 2);
    assertTrue(job.run(true));

    Path outputPath2 = getTempPath(getCallingMethodName() + "2");
    conf = new GiraphConfiguration();
    conf.setComputationClass(WeightedPageRankComputation.class);
    conf.setVertexInputFormatClass(JsonBase64VertexInputFormat.class);
    conf.setVertexOutputFormatClass(JsonBase64VertexOutputFormat.class);
    job = prepareJob(getCallingMethodName(), conf, outputPath2);
    job.getConfiguration().setInt(
        WeightedPageRankComputation.SUPERSTEP_COUNT, 3);
    GiraphFileInputFormat.addVertexInputPath(
      job.getInternalJob().getConfiguration(), outputPath);
    assertTrue(job.run(true));

    Path outputPath3 = getTempPath(getCallingMethodName() + "3");
    conf = new GiraphConfiguration();
    conf.setComputationClass(WeightedPageRankComputation.class);
    conf.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
    conf.setVertexOutputFormatClass(JsonBase64VertexOutputFormat.class);
    job = prepareJob(getCallingMethodName(), conf, outputPath3);
    conf = job.getConfiguration();
    conf.setLong(PseudoRandomInputFormatConstants.AGGREGATE_VERTICES, 101);
    conf.setLong(PseudoRandomInputFormatConstants.EDGES_PER_VERTEX, 2);
    conf.setInt(
        WeightedPageRankComputation.SUPERSTEP_COUNT, 5);
    assertTrue(job.run(true));

    assertEquals(101, getNumResults(conf, outputPath));
    assertEquals(101, getNumResults(conf, outputPath2));
    assertEquals(101, getNumResults(conf, outputPath3));
  }
}
