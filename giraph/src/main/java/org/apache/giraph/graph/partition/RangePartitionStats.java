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

package org.apache.giraph.graph.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Same as {@link PartitionStats}, but also includes the hint for range-based
 * partitioning.
 *
 * @param <I> Vertex index type
 */
@SuppressWarnings("rawtypes")
public class RangePartitionStats<I extends WritableComparable>
    extends PartitionStats {
  /** Can be null if no hint, otherwise a splitting hint */
  private RangeSplitHint<I> hint;

  /**
   * Get the range split hint (if any)
   *
   * @return Hint of how to split the range if desired, null otherwise
   */
  public RangeSplitHint<I> getRangeSplitHint() {
    return hint;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    boolean hintExists = input.readBoolean();
    if (hintExists) {
      hint = new RangeSplitHint<I>();
      hint.readFields(input);
    } else {
      hint = null;
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeBoolean(hint != null);
    if (hint != null) {
      hint.write(output);
    }
  }
}
