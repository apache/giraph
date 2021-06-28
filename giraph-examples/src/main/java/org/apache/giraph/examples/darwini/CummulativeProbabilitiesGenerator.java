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
package org.apache.giraph.examples.darwini;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.Arrays;
import java.util.Random;

/**
 * Helper class to facilitate sampling from
 * the random distribution.
 */
public class CummulativeProbabilitiesGenerator {

  /**
   * Generator's values.
   */
  private DoubleArrayList values = new DoubleArrayList();
  /**
   * Cummulative sum of un-normalized probabilities.
   */
  private DoubleArrayList cummulativeProbabilities = new DoubleArrayList();

  /**
   * Adds a value and a probability (could be un-normalized)
   * to the distribution.
   * @param value value
   * @param probability un-normalized probability
   */
  public void add(double value, double probability) {
    values.add(value);
    double current = 0;
    if (cummulativeProbabilities.size() > 0) {
      current = cummulativeProbabilities.getDouble(
          cummulativeProbabilities.size() - 1);
    }
    cummulativeProbabilities.add(current + probability);
  }

  /**
   * Draw the value from the distribution.
   * @param rnd random number generator
   * @return sampled value
   */
  public double draw(Random rnd) {
    double max = cummulativeProbabilities.getDouble(
        cummulativeProbabilities.size() - 1);
    double rndValue = rnd.nextDouble() * max;
    int position = Arrays.binarySearch(cummulativeProbabilities.elements(), 0,
        cummulativeProbabilities.size(), rndValue);
    if (position < 0) {
      position = - (position + 1);
    }
    return values.getDouble(position);
  }
}
