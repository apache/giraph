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

package org.apache.giraph.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Hint to the {@link RangeMasterPartitioner} about how a
 * {@link RangePartitionOwner} can be split.
 *
 * @param <I> Vertex index to split around
 */
@SuppressWarnings("rawtypes")
public class RangeSplitHint<I extends WritableComparable>
    implements Writable, ImmutableClassesGiraphConfigurable {
  /** Hinted split index */
  private I splitIndex;
  /** Number of vertices in this range before the split */
  private long preSplitVertexCount;
  /** Number of vertices in this range after the split */
  private long postSplitVertexCount;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, ?> conf;

  @Override
  public void readFields(DataInput input) throws IOException {
    splitIndex = conf.createVertexId();
    splitIndex.readFields(input);
    preSplitVertexCount = input.readLong();
    postSplitVertexCount = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    splitIndex.write(output);
    output.writeLong(preSplitVertexCount);
    output.writeLong(postSplitVertexCount);
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return conf;
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
  }
}
