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
package org.apache.giraph.block_app.library.algo;

import org.apache.hadoop.io.Writable;

/**
 * Pair of vertex and value. Used in MultiSeedBreadthFirstSearchBlockFactory
 * for obtaining top N random vertices as seeds.
 *
 * @param <I> Vertex ID type
 */
public class VertexLongPair<I extends Writable>
  implements Comparable<VertexLongPair<I>> {
  private I vertex;
  private long value;

  public VertexLongPair(I vertex, long value) {
    this.vertex = vertex;
    this.value = value;
  }

  public I getVertex() {
    return vertex;
  }

  public void setVertex(I vertex) {
    this.vertex = vertex;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  @Override
  public int compareTo(VertexLongPair<I> other) {
    return Long.compare(this.value, other.getValue());
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof VertexLongPair)) {
      return false;
    }
    VertexLongPair<I> other = (VertexLongPair<I>) object;
    return vertex == other.vertex;
  }

  @Override
  public int hashCode() {
    return vertex.hashCode();
  }
}

