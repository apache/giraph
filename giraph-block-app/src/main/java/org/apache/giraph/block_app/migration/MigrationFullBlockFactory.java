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
package org.apache.giraph.block_app.migration;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.PieceCount;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.migration.MigrationAbstractComputation.MigrationFullAbstractComputation;
import org.apache.giraph.block_app.migration.MigrationMasterCompute.MigrationFullMasterCompute;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.Consumer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

/**
 * BlockFactory to extend when using drop-in migration
 */
public abstract class MigrationFullBlockFactory
    extends AbstractBlockFactory<MigrationSuperstepStage> {

  @Override
  public MigrationSuperstepStage createExecutionStage(
      GiraphConfiguration conf) {
    return new MigrationSuperstepStageImpl();
  }

  @Override
  protected Class<? extends MigrationWorkerContext> getWorkerContextValueClass(
      GiraphConfiguration conf) {
    return MigrationWorkerContext.class;
  }

  @SuppressWarnings("rawtypes")
  public <I extends WritableComparable, V extends Writable, E extends Writable,
  MR extends Writable, MS extends Writable>
  Block createMigrationAppBlock(
      Class<? extends MigrationFullAbstractComputation<I, V, E, MR, MS>>
        computationClass,
      MigrationFullMasterCompute masterCompute,
      Class<MS> messageClass,
      Class<? extends MessageCombiner<? super I, MS>> messageCombinerClass,
      GiraphConfiguration conf) {
    final MigrationPiece<I, V, E, MR, MS> piece =
        MigrationPiece.createFirstFullMigrationPiece(
            computationClass, masterCompute, messageClass,
            messageCombinerClass);
    piece.sanityTypeChecks(conf, null);

    return new SequenceBlock(
        new Piece<WritableComparable, Writable, Writable,
            Writable, MigrationSuperstepStage>() {
          @Override
          public MigrationSuperstepStage nextExecutionStage(
              MigrationSuperstepStage executionStage) {
            return executionStage.changedMigrationSuperstep(0);
          }
        },
        new Block() {
          private MigrationPiece curPiece = piece;

          @Override
          public Iterator<AbstractPiece> iterator() {
            return Iterators.concat(
                Iterators.singletonIterator(curPiece),
                new AbstractIterator<AbstractPiece>() {
                  @Override
                  protected AbstractPiece computeNext() {
                    curPiece = curPiece.getNextPiece();
                    if (curPiece == null) {
                      endOfData();
                    }
                    return curPiece;
                  }
                });
          }

          @Override
          public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
            consumer.apply(curPiece);
          }

          @Override
          public PieceCount getPieceCount() {
            return curPiece.getPieceCount();
          }
        }
    );
  }
}
