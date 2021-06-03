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

package org.apache.giraph.io.superstep_output;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.SimpleVertexWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Class to use as {@link SuperstepOutput} when we don't have output during
 * computation. All the methods are no-ops.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class NoOpSuperstepOutput<I extends WritableComparable,
    V extends Writable, E extends Writable> implements
    SuperstepOutput<I, V, E> {
  @Override
  public SimpleVertexWriter<I, V, E> getVertexWriter() {
    return new SimpleVertexWriter<I, V, E>() {
      @Override
      public void writeVertex(Vertex<I, V, E> vertex) throws IOException,
          InterruptedException {
      }
    };
  }

  @Override
  public void returnVertexWriter(
      SimpleVertexWriter<I, V, E> vertexWriter) {
  }

  @Override
  public void postApplication() {
  }
}
