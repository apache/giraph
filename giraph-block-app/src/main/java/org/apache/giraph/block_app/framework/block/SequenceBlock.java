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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.function.Consumer;

import com.google.common.collect.Iterables;

/**
 * Block that executes provided blocks sequentially.
 */
@SuppressWarnings("rawtypes")
public final class SequenceBlock implements Block {
  private final Block[] blocks;

  public SequenceBlock(Block... blocks) {
    this.blocks = blocks.clone();
  }

  public SequenceBlock(List<? extends Block> blocks) {
    this.blocks = blocks.toArray(new Block[blocks.size()]);
  }

  @Override
  public Iterator<AbstractPiece> iterator() {
    return Iterables.concat(Arrays.asList(blocks)).iterator();
  }

  @Override
  public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
    for (Block block : blocks) {
      block.forAllPossiblePieces(consumer);
    }
  }

  @Override
  public PieceCount getPieceCount() {
    PieceCount ret = new PieceCount(0);
    for (Block block : blocks) {
      ret.add(block.getPieceCount());
    }
    return ret;
  }

  @Override
  public String toString() {
    return "SequenceBlock" + Arrays.toString(blocks);
  }
}
