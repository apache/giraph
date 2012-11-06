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

package org.apache.giraph.comm;

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.PairListWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Holder for pairs of vertex ids and messages. Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class VertexIdMessageCollection<I extends WritableComparable,
    M extends Writable> extends PairListWritable<I, M> {
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> conf;

  /**
   * Constructor.
   * Doesn't create the inner lists. If the object is going to be
   * deserialized lists will be created in {@code readFields()},
   * otherwise you should call {@code initialize()} before using this object.
   *
   * @param conf Giraph configuration
   */
  public VertexIdMessageCollection(
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> conf) {
    this.conf = conf;
  }

  @Override
  protected I newFirstInstance() {
    return conf.createVertexId();
  }

  @Override
  protected M newSecondInstance() {
    return conf.createMessageValue();
  }
}
