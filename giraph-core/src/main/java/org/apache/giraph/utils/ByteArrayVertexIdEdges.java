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
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Stores vertex id and edge pairs in a single byte array.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
@SuppressWarnings("unchecked")
public class ByteArrayVertexIdEdges<I extends WritableComparable,
    E extends Writable> extends ByteArrayVertexIdData<I, Edge<I,
    E>> implements VertexIdEdges<I, E> {
  /**
   * Cast the {@link ImmutableClassesGiraphConfiguration} so it can be used
   * to generate edge objects.
   *
   * @return Casted configuration
   */
  @Override
  public ImmutableClassesGiraphConfiguration<I, ?, E> getConf() {
    return (ImmutableClassesGiraphConfiguration<I, ?, E>) super.getConf();
  }

  @Override
  public Edge<I, E> createData() {
    return getConf().createReusableEdge();
  }

  @Override
  public void writeData(ExtendedDataOutput out, Edge<I, E> edge)
    throws IOException {
    WritableUtils.writeEdge(out, edge);
  }

  @Override
  public void readData(ExtendedDataInput in, Edge<I, E> edge)
    throws IOException {
    WritableUtils.readEdge(in, edge);
  }

  @Override
  public ByteStructVertexIdEdgeIterator<I, E> getVertexIdEdgeIterator() {
    return new ByteStructVertexIdEdgeIterator<>(this);
  }
}

