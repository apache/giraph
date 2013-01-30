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

package org.apache.giraph.graph;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;

/**
 * An edge that has no value.
 *
 * @param <I> Vertex ID
 */
public class EdgeNoValue<I extends WritableComparable>
    implements MutableEdge<I, NullWritable> {
  /** Target vertex id */
  private I targetVertexId = null;

  /** Empty constructor */
  public EdgeNoValue() {
    // do nothing
  }

  /**
   * Constructor with target vertex ID
   * @param targetVertexId vertex ID
   */
  public EdgeNoValue(I targetVertexId) {
    this.targetVertexId = targetVertexId;
  }

  @Override
  public void setTargetVertexId(I targetVertexId) {
    this.targetVertexId = targetVertexId;
  }

  @Override
  public void setValue(NullWritable value) {
    // do nothing
  }

  @Override
  public I getTargetVertexId() {
    return targetVertexId;
  }

  @Override
  public NullWritable getValue() {
    return NullWritable.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EdgeNoValue edge = (EdgeNoValue) o;
    return Objects.equals(targetVertexId, edge.targetVertexId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetVertexId);
  }

  @Override
  public int compareTo(Edge<I, NullWritable> o) {
    return targetVertexId.compareTo(o.getTargetVertexId());
  }
}
