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

package org.apache.giraph.io;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract base class for VertexValueReader.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 */
public abstract class BasicVertexValueReader<I extends WritableComparable,
    V extends Writable>
    extends VertexReader<I, V, Writable> {
  /**
   * User-defined method to extract the vertex id.
   *
   * @return The vertex id
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  public abstract I getCurrentVertexId() throws IOException,
      InterruptedException;

  /**
   * User-defined method to extract the vertex value.
   *
   * @return The vertex value
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract V getCurrentVertexValue() throws IOException,
      InterruptedException;
}
