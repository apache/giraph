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

/**
 * Block that executes one of two branches based on a condition
 */
@SuppressWarnings("rawtypes")
public final class IfBlock implements Block {
  private final Block thenBlock;
  private final Block elseBlock;
  private final Supplier<Boolean> condition;

  public IfBlock(
      Supplier<Boolean> condition, Block thenBlock, Block elseBlock) {
    this.condition = condition;
    this.thenBlock = thenBlock;
    this.elseBlock = elseBlock;
  }

  public IfBlock(Supplier<Boolean> condition, Block thenBlock) {
    this.condition = condition;
    this.thenBlock = thenBlock;
    this.elseBlock = new EmptyBlock();
  }

  @Override
  public Iterator<AbstractPiece> iterator() {
    if (Boolean.TRUE.equals(condition.get())) {
      return thenBlock.iterator();
    } else {
      return elseBlock.iterator();
    }
  }

  @Override
  public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
    thenBlock.forAllPossiblePieces(consumer);
    elseBlock.forAllPossiblePieces(consumer);
  }

  @Override
  public PieceCount getPieceCount() {
    PieceCount thenCount = thenBlock.getPieceCount();
    PieceCount elseCount = elseBlock.getPieceCount();
    return thenCount.equals(elseCount) ?
        thenCount : PieceCount.createUnknownCount();
  }

  @Override
  public String toString() {
    if (elseBlock instanceof EmptyBlock) {
      return "IfBlock(" + thenBlock + ")";
    }
    return "IfBlock(" + thenBlock + " , " + elseBlock + ")";
  }
}
