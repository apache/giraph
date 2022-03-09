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
import org.apache.giraph.function.Consumer;

/**
 * Composable unit of execution. Used to combine other Blocks into
 * bigger units. Each Piece represents a Block itself.
 *
 * Execution is represented as an iterator across Pieces.
 *
 * The whole application run is represented by a single block at the end.
 */
@SuppressWarnings("rawtypes")
public interface Block extends Iterable<AbstractPiece> {
  /**
   * Create iterator representing all pieces needed to be executed
   * in this block.
   *
   * After Iterator.next call returns, master compute of returned Piece is
   * guaranteed to be called before calling hasNext/next on the iterator.
   * (allows for iterators logic to depend on the execution dynamically,
   * and not be only static)
   */
  @Override
  Iterator<AbstractPiece> iterator();

  /**
   * Calls consumer for each Piece:
   * - in no particular order
   * - potentially calling multiple times on same Piece
   * - even if Piece might never be returned in the iterator
   * - it will be called at least once for every piece that is
   *   going to be returned by iterator
   *
   * Can be used for static analysis/introspection of the block,
   * without actually executing them.
   */
  void forAllPossiblePieces(Consumer<AbstractPiece> consumer);

  /**
   * How many pieces are in this block.
   * Sometimes we don't know (eg RepeatBlock).
   *
   * @return How many pieces are in this block.
   */
  PieceCount getPieceCount();
}
