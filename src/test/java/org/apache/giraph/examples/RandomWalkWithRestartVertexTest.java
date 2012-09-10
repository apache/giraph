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

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.giraph.examples.RandomWalkVertex.
  RandomWalkVertexMasterCompute;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Tests for {@link RandomWalkWithRestartVertex}
 */
public class RandomWalkWithRestartVertexTest {

  /** Minimum difference between doubles */
  private static final double EPSILON = 10e-3;

  /**
   * A local integration test on toy data
   */
  @Test
  public void testToyData() throws Exception {

    // A small graph
    String[] graph = new String[] { "12 34 56", "34 78", "56 34 78", "78 34" };

    Map<String, String> params = Maps.newHashMap();
    params.put(RandomWalkWithRestartVertex.SOURCE_VERTEX, "12");
    params.put(RandomWalkWithRestartVertex.MAX_SUPERSTEPS, "30");
    params.put(RandomWalkWithRestartVertex.TELEPORTATION_PROBABILITY, "0.25");

    // Run internally
    Iterable<String> results =
        InternalVertexRunner.run(RandomWalkWithRestartVertex.class, null,
            LongDoubleFloatDoubleTextInputFormat.class,
            VertexWithDoubleValueFloatEdgeTextOutputFormat.class,
            RandomWalkWorkerContext.class, RandomWalkVertexMasterCompute.class,
            params, graph);

    Map<Long, Double> steadyStateProbabilities =
        parseSteadyStateProbabilities(results);
    // values computed with external software
    // 0.25, 0.354872, 0.09375, 0.301377
    assertEquals(0.25, steadyStateProbabilities.get(12L), EPSILON);
    assertEquals(0.354872, steadyStateProbabilities.get(34L), EPSILON);
    assertEquals(0.09375, steadyStateProbabilities.get(56L), EPSILON);
    assertEquals(0.301377, steadyStateProbabilities.get(78L), EPSILON);
  }

  /**
   * A local integration test on toy data
   */
  @Test
  public void testWeightedGraph() throws Exception {
    // A small graph
    String[] graph =
        new String[] { "12 34:0.1 56:0.9", "34 78:0.9 56:0.1",
          "56 12:0.1 34:0.8 78:0.1", "78 34:1.0" };

    Map<String, String> params = Maps.newHashMap();
    params.put(RandomWalkWithRestartVertex.SOURCE_VERTEX, "12");
    params.put(RandomWalkWithRestartVertex.MAX_SUPERSTEPS, "30");
    params.put(RandomWalkWithRestartVertex.TELEPORTATION_PROBABILITY, "0.15");

    // Run internally
    Iterable<String> results =
        InternalVertexRunner.run(RandomWalkWithRestartVertex.class, null,
            NormalizingLongDoubleFloatDoubleTextInputFormat.class,
            VertexWithDoubleValueFloatEdgeTextOutputFormat.class,
            RandomWalkWorkerContext.class, RandomWalkVertexMasterCompute.class,
            params, graph);

    Map<Long, Double> steadyStateProbabilities =
        parseSteadyStateProbabilities(results);
    // values computed with external software
    // 0.163365, 0.378932, 0.156886, 0.300816
    assertEquals(0.163365, steadyStateProbabilities.get(12L), EPSILON);
    assertEquals(0.378932, steadyStateProbabilities.get(34L), EPSILON);
    assertEquals(0.156886, steadyStateProbabilities.get(56L), EPSILON);
    assertEquals(0.300816, steadyStateProbabilities.get(78L), EPSILON);
  }

  /**
   * Parse steady state probabilities.
   * @param results The steady state probabilities in text format.
   * @return A map representation of the steady state probabilities.
   */
  private Map<Long, Double> parseSteadyStateProbabilities(
      Iterable<String> results) {
    Map<Long, Double> result = Maps.newHashMap();
    for (String s : results) {
      String[] tokens = s.split("\\t");
      Long id = Long.parseLong(tokens[0]);
      Double value = Double.parseDouble(tokens[1]);
      result.put(id, value);
    }
    return result;
  }
}
