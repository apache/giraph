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

import org.apache.giraph.examples.AggregatorsTestVertex;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.giraph.graph.GiraphJob;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

/** Tests if aggregators are handled on a proper way during supersteps */
public class TestAggregatorsHandling extends BspCase {

  public TestAggregatorsHandling() {
    super(TestAggregatorsHandling.class.getName());
  }

  @Test
  public void testAggregatorsHandling() throws IOException,
      ClassNotFoundException, InterruptedException {
    GiraphJob job = prepareJob(getCallingMethodName(),
        AggregatorsTestVertex.class,
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
    job.setMasterComputeClass(
        AggregatorsTestVertex.AggregatorsTestMasterCompute.class);
    job.getConfiguration().setBoolean(GiraphJob.USE_NETTY, true);
    assertTrue(job.run(true));
  }
}
