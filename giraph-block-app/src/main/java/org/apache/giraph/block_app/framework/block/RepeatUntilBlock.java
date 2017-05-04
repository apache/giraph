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
import org.apache.giraph.function.Supplier;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

/**
 * Block that repeats another block until toQuit supplier returns true,
 * but at most given number of times.
 *
 * If toQuit returns true on first run, block is not going
 * to be executed at all.
 */
@SuppressWarnings("rawtypes")
public final class RepeatUntilBlock implements Block {
  private final Block block;
  private final int repeatTimes;
  private final Supplier<Boolean> toQuit;

  public RepeatUntilBlock(
      int repeatTimes, Block block, Supplier<Boolean> toQuit) {
    this.block = block;
    this.repeatTimes = repeatTimes;
    this.toQuit = toQuit;
  }

  /**
   * Repeat unlimited number of times, until toQuit supplier returns true.
   */
  public static Block unlimited(Block block, Supplier<Boolean> toQuit) {
    return new RepeatUntilBlock(Integer.MAX_VALUE, block, toQuit);
  }

  @Override
  public Iterator<AbstractPiece> iterator() {
    return Iterators.concat(new AbstractIterator<Iterator<AbstractPiece>>() {
      private int index = 0;

      @Override
      protected Iterator<AbstractPiece> computeNext() {
        if (index >= repeatTimes || Boolean.TRUE.equals(toQuit.get())) {
          return endOfData();
        }
        index++;
        return block.iterator();
      }
    });
  }

  @Override
  public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
    block.forAllPossiblePieces(consumer);
  }

  @Override
  public PieceCount getPieceCount() {
    return PieceCount.createUnknownCount();
  }

  @Override
  public String toString() {
    return "RepeatUntilBlock(" + repeatTimes + " * " + block + ")";
  }
}
