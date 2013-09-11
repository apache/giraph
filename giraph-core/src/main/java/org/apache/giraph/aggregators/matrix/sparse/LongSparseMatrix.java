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

package org.apache.giraph.aggregators.matrix.sparse;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * A long matrix holds the values of the entries in long vectors. It keeps one
 * long aggregator per matrix row.
 */
public class LongSparseMatrix {
  /** The number of rows in the matrix */
  private int numRows;
  /** The rows of the matrix */
  private Int2ObjectOpenHashMap<LongSparseVector> rows;

  /**
   * Create a new matrix with the given number of rows.
   *
   * @param numRows the number of rows.
   */
  public LongSparseMatrix(int numRows) {
    this.numRows = numRows;
    rows = new Int2ObjectOpenHashMap<LongSparseVector>(numRows);
    rows.defaultReturnValue(null);
  }

  /**
   * Create a empty matrix with all values set to 0.0
   */
  public void initialize() {
    rows.clear();
    for (int i = 0; i < numRows; ++i) {
      setRow(i, new LongSparseVector());
    }
  }

  /**
   * Get the number of rows in the matrix.
   *
   * @return the number of rows.
   */
  public int getNumRows() {
    return numRows;
  }

  /**
   * Get a specific entry of the matrix.
   *
   * @param i the row
   * @param j the column
   * @return the value of the entry
   */
  public long get(int i, int j) {
    return rows.get(i).get(j);
  }

  /**
   * Set a specific entry of the matrix.
   *
   * @param i the row
   * @param j the column
   * @param v the value of the entry
   */
  public void set(int i, int j, long v) {
    rows.get(i).set(j, v);
  }

  /**
   * Get a specific row of the matrix.
   *
   * @param i the row number
   * @return the row of the matrix
   */
  LongSparseVector getRow(int i) {
    return rows.get(i);
  }

  /**
   * Set the long vector as the row specified.
   *
   * @param i the row
   * @param vec the vector to set as the row
   */
  void setRow(int i, LongSparseVector vec) {
    rows.put(i, vec);
  }
}
