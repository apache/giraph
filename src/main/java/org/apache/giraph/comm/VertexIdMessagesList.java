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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Wrapper around {@link ArrayListWritable} that provides the list for
 * {@link VertexIdMessages}.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class VertexIdMessagesList<I extends WritableComparable,
    M extends Writable> extends ArrayListWritable<VertexIdMessages<I, M>> {
  /** Defining a layout version for a serializable class. */
  private static final long serialVersionUID = 100L;

  /**
   * Default constructor.
   */
  public VertexIdMessagesList() {
    super();
  }

  /**
   * Copy constructor.
   *
   * @param vertexIdMessagesList List to be copied.
   */
  public VertexIdMessagesList(VertexIdMessagesList<I, M> vertexIdMessagesList) {
    super(vertexIdMessagesList);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setClass() {
    setClass((Class<VertexIdMessages<I, M>>)
      (new VertexIdMessages<I, M>()).getClass());
  }
}
