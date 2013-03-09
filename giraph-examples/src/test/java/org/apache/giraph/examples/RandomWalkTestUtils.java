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

import com.google.common.collect.Maps;

import java.util.Map;

public class RandomWalkTestUtils {

  /** Minimum difference between doubles */
  public static final double EPSILON = 10e-3;

  /**
   * Parse steady state probabilities.
   * @param results The steady state probabilities in text format.
   * @return A map representation of the steady state probabilities.
   */
  public static Map<Long, Double> parseSteadyStateProbabilities(
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
