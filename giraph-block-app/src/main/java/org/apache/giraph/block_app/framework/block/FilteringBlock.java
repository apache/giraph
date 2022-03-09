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
package org.apache.giraph.block_app.framework.block;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.delegate.FilteringPiece;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Block which filters out calls to vertexSend/vertexReceive functions
 * of all pieces in a given block.
 * Filtering happens based on toCallSend and toCallReceive suppliers
 * that are passed in, as every piece is just wrapped with FilteringPiece.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public final class FilteringBlock<I extends WritableComparable,
        V extends Writable, E extends Writable>
    implements Block {
  private final SupplierFromVertex<I, V, E, Boolean> toCallSend;
  private final SupplierFromVertex<I, V, E, Boolean> toCallReceive;
  private final Block block;

  /**
   * Creates filtering block which uses passed {@code toCallSend} to filter
   * calls to {@code vertexSend}, and passed {@code toCallReceive} to filter
   * calls to {@code vertexReceive}, on all pieces within passed {@code block}.
   */
  public FilteringBlock(
      SupplierFromVertex<I, V, E, Boolean> toCallSend,
      SupplierFromVertex<I, V, E, Boolean> toCallReceive,
      Block block) {
    this.toCallSend = toCallSend;
    this.toCallReceive = toCallReceive;
    this.block = block;
  }

  /**
   * Creates filtering block, where both vertexSend and vertexReceive is
   * filtered based on same supplier.
   */
  public FilteringBlock(
      SupplierFromVertex<I, V, E, Boolean> toCallSendAndReceive, Block block) {
    this(toCallSendAndReceive, toCallSendAndReceive, block);
  }

  /**
   * Creates filtering block, that filters only vertexReceive function,
   * and always calls vertexSend function.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  Block createReceiveFiltering(
      SupplierFromVertex<I, V, E, Boolean> toCallReceive,
      Block innerBlock) {
    return new FilteringBlock<>(null, toCallReceive, innerBlock);
  }

  /**
   * Creates filtering block, that filters only vertexSend function,
   * and always calls vertexReceive function.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  Block createSendFiltering(
      SupplierFromVertex<I, V, E, Boolean> toCallSend,
      Block innerBlock) {
    return new FilteringBlock<>(toCallSend, null, innerBlock);
  }

  @Override
  public Iterator<AbstractPiece> iterator() {
    return Iterators.transform(
        block.iterator(),
        new Function<AbstractPiece, AbstractPiece>() {
          @Override
          public AbstractPiece apply(AbstractPiece input) {
            return new FilteringPiece<>(toCallSend, toCallReceive, input);
          }
        });
  }

  @Override
  public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
    block.forAllPossiblePieces(consumer);
  }

  @Override
  public PieceCount getPieceCount() {
    return block.getPieceCount();
  }
}
