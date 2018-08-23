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

package org.apache.giraph.aggregators.matrix;

import java.util.ArrayList;

/**
 * The abstract matrix aggregator contains the prefix name of the vector
 * aggregators that have the values of the rows. It also cashes the names to
 * avoid creating the same string multiples times.
 */
public abstract class MatrixSumAggregator {
  /**
   * The prefix name of the vector aggregators. The aggregator names are created
   * as (name0, name1, ...).
   */
  private String name;
  /** Cache the names of the columns */
  private ArrayList<String> names = new ArrayList<>();

  /**
   * Create a new matrix aggregator with the given prefix name for the vector
   * aggregators.
   *
   * @param name the prefix for the row vector aggregators
   */
  public MatrixSumAggregator(String name) {
    this.name = name;
  }

  /**
   * Get the name of the aggreagator of the row with the index specified.
   *
   * @param i the row of the matrix
   * @return the name of the aggregator
   */
  protected String getRowAggregatorName(int i) {
    for (int n = names.size(); n <= i; ++n) {
      names.add(name + n);
    }
    return names.get(i);
  }
}
