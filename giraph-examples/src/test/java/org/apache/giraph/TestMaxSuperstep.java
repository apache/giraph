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
import org.apache.giraph.counters.GiraphHadoopCounter;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.examples.SimplePageRankVertex.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimplePageRankVertex.SimplePageRankVertexOutputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for testing max superstep feature of Giraph
 */
public class TestMaxSuperstep extends BspCase {
  public TestMaxSuperstep() {
      super(TestMaxSuperstep.class.getName());
  }

  /**
   * Simple test vertex class that will run forever (voteToHalt is never
   * called).
   */
  public static class InfiniteLoopVertex extends Vertex<LongWritable,
      DoubleWritable, FloatWritable, DoubleWritable> {
    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
      // Do nothing, run forever!
    }
  }

  /**
   * Run a job that tests that this job completes in the desired number of
   * supersteps
   *
   * @throws java.io.IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testMaxSuperstep()
          throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexClass(InfiniteLoopVertex.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf,
        getTempPath(getCallingMethodName()));
    job.getConfiguration().setMaxNumberOfSupersteps(3);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      GiraphHadoopCounter superstepCounter =
          GiraphStats.getInstance().getSuperstepCounter();
      assertEquals(superstepCounter.getValue(), 3L);
    }
  }
}
