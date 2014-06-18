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
public abstract class ByteStructVertexIdIterator<I extends WritableComparable>
  implements VertexIdIterator<I> {
  /** Reader of the serialized edges */
  protected final ExtendedDataInput extendedDataInput;

  /** Current vertex id */
  protected I vertexId;

  /**
   * Constructor.
   *
   * @param extendedDataOutput Extended data output
   * @param conf Configuration
   */
  public ByteStructVertexIdIterator(
    ExtendedDataOutput extendedDataOutput,
    ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
    if (extendedDataOutput != null && conf != null) {
      extendedDataInput = conf.createExtendedDataInput(extendedDataOutput);
    } else {
      throw new IllegalStateException("Cannot instantiate vertexIdIterator " +
        "with null arguments");
    }
  }

  @Override
  public boolean hasNext() {
    return !extendedDataInput.endOfInput();
  }

  @Override
  public I getCurrentVertexId() {
    return vertexId;
  }

  @Override
  public I releaseCurrentVertexId() {
    I releasedVertexId = vertexId;
    vertexId = null;
    return releasedVertexId;
  }
}
