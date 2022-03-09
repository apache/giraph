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
package org.apache.giraph.block_app.library.prepare_graph.vertex;

import org.apache.giraph.block_app.library.ReusableSuppliers;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Vertex value interface for connected component extraction
 */
public interface ConnectedComponentVertexValue extends Writable {
  long getComponent();
  void setComponent(long component);

  /**
   * If your vertex value class implements ConnectedComponentVertexValue,
   * you can use this consumer for the UndirectedConnectedComponent Blocks.
   */
  static
  <I extends WritableComparable, V extends ConnectedComponentVertexValue,
  E extends Writable>
  ConsumerWithVertex<I, V, E, LongWritable> setComponentConsumer() {
    return (vertex, value) -> vertex.getValue().setComponent(value.get());
  }

  /**
   *  If your vertex value class implements ConnectedComponentVertexValue, you
   * can use this supplier for the UndirectedConnectedComponent Blocks.
   */
  static
  <I extends WritableComparable, V extends ConnectedComponentVertexValue,
  E extends Writable>
  SupplierFromVertex<I, V, E, LongWritable> getComponentSupplier() {
    return ReusableSuppliers.fromLong(
        (vertex) -> vertex.getValue().getComponent());
  }
}
