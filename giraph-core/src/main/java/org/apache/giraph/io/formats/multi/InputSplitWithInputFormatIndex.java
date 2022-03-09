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

package org.apache.giraph.io.formats.multi;


import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;

/**
 * Helper InputSplit class which holds the information about input format
 * which owns this input split.
 *
 * Used only with input formats which wrap several input formats into one
 * ({@link MultiVertexInputFormat} and {@link MultiEdgeInputFormat})
 */
class InputSplitWithInputFormatIndex extends InputSplit {
  /** Wrapped input split */
  private InputSplit split;
  /** Index of input format which owns the input split */
  private int inputFormatIndex;

  /**
   * Constructor
   *
   * @param split            Input split
   * @param inputFormatIndex Index of input format which owns the input split
   */
  InputSplitWithInputFormatIndex(InputSplit split, int inputFormatIndex) {
    this.inputFormatIndex = inputFormatIndex;
    this.split = split;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return split.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return split.getLocations();
  }

  int getInputFormatIndex() {
    return inputFormatIndex;
  }

  InputSplit getSplit() {
    return split;
  }
}
