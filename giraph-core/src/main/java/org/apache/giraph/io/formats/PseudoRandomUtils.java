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

package org.apache.giraph.io.formats;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for PseudoRandom input formats
 */
public class PseudoRandomUtils {
  /** Do not instantiate */
  private PseudoRandomUtils() { }

  /**
   * Create desired number of {@link BspInputSplit}s
   *
   * @param numSplits How many splits to create
   * @return List of {@link BspInputSplit}s
   */
  public static List<InputSplit> getSplits(int numSplits) throws IOException,
      InterruptedException {
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    for (int i = 0; i < numSplits; ++i) {
      inputSplitList.add(new BspInputSplit(i, numSplits));
    }
    return inputSplitList;
  }
}
