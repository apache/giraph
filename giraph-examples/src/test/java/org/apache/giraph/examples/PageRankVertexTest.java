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

package org.apache.giraph.examples;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 * Tests for {@link PageRankVertex}
 */
public class PageRankVertexTest {

  /**
   * A local integration test on toy data
   */
  @Test
  public void testToyData() throws Exception {

    // A small graph
    String[] graph = new String[] {
      "1 4 2 3",
      "2 1",
      "3",
      "4 3 2",
      "5 2 4"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setInt(RandomWalkWithRestartVertex.MAX_SUPERSTEPS, 50);
    conf.setFloat(RandomWalkWithRestartVertex.TELEPORTATION_PROBABILITY, 0.15f);
    conf.setVertexClass(PageRankVertex.class);
    conf.setVertexEdgesClass(ByteArrayEdges.class);
    conf.setVertexInputFormatClass(LongDoubleNullTextInputFormat.class);
    conf.setVertexOutputFormatClass(
        VertexWithDoubleValueNullEdgeTextOutputFormat.class);
    conf.setWorkerContextClass(RandomWalkWorkerContext.class);
    conf.setMasterComputeClass(
        RandomWalkVertex.RandomWalkVertexMasterCompute.class);
    // Run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    Map<Long, Double> steadyStateProbabilities =
        RandomWalkTestUtils.parseSteadyStateProbabilities(results);

    assertEquals(0.28159076008518047, steadyStateProbabilities.get(1l),
        RandomWalkTestUtils.EPSILON);
    assertEquals(0.2514648601529863, steadyStateProbabilities.get(2l),
        RandomWalkTestUtils.EPSILON);
    assertEquals(0.22262961972286327, steadyStateProbabilities.get(3l),
        RandomWalkTestUtils.EPSILON);
    assertEquals(0.17646783276703806, steadyStateProbabilities.get(4l),
        RandomWalkTestUtils.EPSILON);
    assertEquals(0.06784692727193153, steadyStateProbabilities.get(5l),
        RandomWalkTestUtils.EPSILON);
  }
}
