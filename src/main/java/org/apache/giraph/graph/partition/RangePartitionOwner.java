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

import org.apache.giraph.graph.BspUtils;
import org.apache.hadoop.io.WritableComparable;

/**
 * Added the max key index in to the {@link PartitionOwner}.  Also can provide
 * a split hint if desired.
 *
 * @param <I> Vertex index type
 */
@SuppressWarnings("rawtypes")
public class RangePartitionOwner<I extends WritableComparable>
    extends BasicPartitionOwner {
  /** Max index for this partition */
  private I maxIndex;

  /**
   * Default constructor.
   */
  public RangePartitionOwner() { }

  /**
   * Constructor with the max index.
   *
   * @param maxIndex Max index of this partition.
   */
  public RangePartitionOwner(I maxIndex) {
    this.maxIndex = maxIndex;
  }

  /**
   * Get the maximum index of this partition owner.
   *
   * @return Maximum index.
   */
  public I getMaxIndex() {
    return maxIndex;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    maxIndex = BspUtils.<I>createVertexIndex(getConf());
    maxIndex.readFields(input);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    maxIndex.write(output);
  }
}
