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

import org.apache.giraph.io.GiraphInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Utility methods used by {@link MultiVertexInputFormat} and
 * {@link MultiEdgeInputFormat}
 */
public class MultiInputUtils {
  /** Do not instantiate */
  private MultiInputUtils() {
  }

  /**
   * Get the list of input splits for all formats.
   *
   * @param context The job context
   * @param minSplitCountHint Minimum number of splits to create (hint)
   * @param inputFormats List of input formats
   * @return The list of input splits
   */
  public static List<InputSplit> getSplits(JobContext context,
      int minSplitCountHint,
      List<? extends GiraphInputFormat> inputFormats) throws IOException,
      InterruptedException {
    List<InputSplit> splits = Lists.newArrayList();
    for (int index = 0; index < inputFormats.size(); index++) {
      List<InputSplit> inputFormatSplits =
          inputFormats.get(index).getSplits(context, minSplitCountHint);
      for (InputSplit split : inputFormatSplits) {
        splits.add(new InputSplitWithInputFormatIndex(split, index));
      }
    }
    return splits;
  }

  /**
   * Write input split info to DataOutput. Input split belongs to one of the
   * formats.
   *
   * @param inputSplit InputSplit
   * @param dataOutput DataOutput
   * @param inputFormats List of input formats
   */
  public static void writeInputSplit(InputSplit inputSplit,
      DataOutput dataOutput,
      List<? extends GiraphInputFormat> inputFormats) throws IOException {
    if (inputSplit instanceof InputSplitWithInputFormatIndex) {
      InputSplitWithInputFormatIndex split =
          (InputSplitWithInputFormatIndex) inputSplit;
      int index = split.getInputFormatIndex();
      dataOutput.writeInt(index);
      inputFormats.get(index).writeInputSplit(split.getSplit(), dataOutput);
    } else {
      throw new IllegalStateException("writeInputSplit: Got InputSplit which " +
          "was not created by multi input: " + inputSplit.getClass().getName());
    }
  }

  /**
   * Read input split info from DataInput. Input split belongs to one of the
   * formats.
   *
   * @param dataInput DataInput
   * @param inputFormats List of input formats
   * @return InputSplit
   */
  public static InputSplit readInputSplit(
      DataInput dataInput,
      List<? extends GiraphInputFormat> inputFormats) throws IOException,
      ClassNotFoundException {
    int index = dataInput.readInt();
    InputSplit split = inputFormats.get(index).readInputSplit(dataInput);
    return new InputSplitWithInputFormatIndex(split, index);
  }
}
