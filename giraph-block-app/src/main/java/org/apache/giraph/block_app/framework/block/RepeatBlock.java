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

import java.util.Collections;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.primitive.IntSupplier;

import com.google.common.collect.Iterables;

/**
 * Block that repeats another block given number of times.
 */
@SuppressWarnings("rawtypes")
public final class RepeatBlock implements Block {
  private final Block block;
  private final boolean constantRepeatTimes;
  private final IntSupplier repeatTimes;

  public RepeatBlock(final int repeatTimes, Block block) {
    this.block = block;
    this.constantRepeatTimes = true;
    this.repeatTimes = new IntSupplier() {
      @Override
      public int get() {
        return repeatTimes;
      }
    };
  }

  /**
   * Creates a repeat block, that before starting execution takes number of
   * iterations from the given supplier.
   *
   * This allows number of iterations to be dynamic, and depend on
   * execution that happens before.
   * Note - it doesn't allow for number of repetitions to change during the
   * loop itself - as it is supplier is called only when this block gets
   * its turn.
   */
  public RepeatBlock(IntSupplier repeatTimes, Block block) {
    this.block = block;
    this.constantRepeatTimes = false;
    this.repeatTimes = repeatTimes;
  }

  /**
   * Create a repeat block that executes unlimited number of times.
   *
   * Should rarely be used, as it will cause application never to finish,
   * unless other unconventional ways of termination are used.
   */
  public static Block unlimited(Block block) {
    return new RepeatBlock(Integer.MAX_VALUE, block);
  }

  @Override
  public Iterator<AbstractPiece> iterator() {
    return Iterables.concat(
        Collections.nCopies(repeatTimes.get(), block)).iterator();
  }

  @Override
  public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
    block.forAllPossiblePieces(consumer);
  }

  @Override
  public PieceCount getPieceCount() {
    return constantRepeatTimes ?
        block.getPieceCount().multiply(repeatTimes.get()) :
        PieceCount.createUnknownCount();
  }

  @Override
  public String toString() {
    return "RepeatBlock(" + repeatTimes + " * " + block + ")";
  }
}
