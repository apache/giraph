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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Basic partition class for other partitions to extend. Holds partition id,
 * configuration, progressable and partition context
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class BasicPartition<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements Partition<I, V, E, M> {
  /** Configuration from the worker */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Partition id */
  private int id;
  /** Context used to report progress */
  private Progressable progressable;
  /** Partition context */
  private PartitionContext partitionContext;

  @Override
  public void initialize(int partitionId, Progressable progressable) {
    setId(partitionId);
    setProgressable(progressable);
    partitionContext = conf.createPartitionContext();
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration) {
    conf = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, V, E, M> getConf() {
    return conf;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void setId(int id) {
    this.id = id;
  }

  @Override
  public PartitionContext getPartitionContext() {
    return partitionContext;
  }

  @Override
  public void progress() {
    if (progressable != null) {
      progressable.progress();
    }
  }

  @Override
  public void setProgressable(Progressable progressable) {
    this.progressable = progressable;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(id);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    id = input.readInt();
    partitionContext = conf.createPartitionContext();
  }
}
