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

package org.apache.giraph.utils;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.WritableComparable;

/**
 * Common implementation for VertexIdEdgeIterator, VertexIdMessageIterator
 * and VertexIdMessageBytesIterator.
 *
 * @param <I> Vertex id
 */
public abstract class VertexIdIterator<I extends WritableComparable> {
  /** Reader of the serialized edges */
  protected final ExtendedDataInput extendedDataInput;

  /** Current vertex id */
  protected I vertexId;

  /**
   * Constructor.
   *
   * @param extendedDataOutput Extended data output
   * @param configuration Configuration
   */
  public VertexIdIterator(
      ExtendedDataOutput extendedDataOutput,
      ImmutableClassesGiraphConfiguration<I, ?, ?> configuration) {
    extendedDataInput = configuration.createExtendedDataInput(
        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
  }

  /**
   * Returns true if the iteration has more elements.
   *
   * @return True if the iteration has more elements.
   */
  public boolean hasNext() {
    return extendedDataInput.available() > 0;
  }
  /**
   * Moves to the next element in the iteration.
   */
  public abstract void next();

  /**
   * Get the current vertex id.  Ihis object's contents are only guaranteed
   * until next() is called.  To take ownership of this object call
   * releaseCurrentVertexId() after getting a reference to this object.
   *
   * @return Current vertex id
   */
  public I getCurrentVertexId() {
    return vertexId;
  }
  /**
   * The backing store of the current vertex id is now released.
   * Further calls to getCurrentVertexId () without calling next()
   * will return null.
   *
   * @return Current vertex id that was released
   */
  public I releaseCurrentVertexId() {
    I releasedVertexId = vertexId;
    vertexId = null;
    return releasedVertexId;
  }
}
