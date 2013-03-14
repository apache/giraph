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

package org.apache.giraph.edge;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Map;

/**
 * Helper class for a mutable edge that modifies the backing map entry.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class MapMutableEdge<I extends WritableComparable, E extends Writable>
    implements MutableEdge<I, E> {
  /** Backing entry for the edge in the map. */
  private Map.Entry<I, E> entry;

  /**
   * Set the backing entry for this edge in the map.
   *
   * @param entry Backing entry
   */
  public void setEntry(Map.Entry<I, E> entry) {
    this.entry = entry;
  }

  @Override
  public void setValue(E value) {
    // Replace the value in the map.
    entry.setValue(value);
  }

  @Override
  public I getTargetVertexId() {
    return entry.getKey();
  }

  @Override
  public E getValue() {
    return entry.getValue();
  }
}
