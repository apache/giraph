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
package org.apache.giraph.block_app.examples.pagerank;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * PageRank example of using BlockFactory - in it's simplest form.
 * This single class represents everything needed for the application.
 * To use it - set BlockUtils.BLOCK_FACTORY_CLASS property to this class.
 *
 * Note, as a general practice, for iteration Piece to be reusable, it should not
 * assume fixed vertex value class, but specify interface it needs, and use it instead.
 */
public class PageRankWithPiecesExampleBlockFactory extends AbstractPageRankExampleBlockFactory {
  @Override
  public Block createBlock(GiraphConfiguration conf) {
    return new RepeatBlock(NUM_ITERATIONS.get(conf), new PageRankUpdatePiece());
  }

  /** One PageRank iteration */
  @SuppressWarnings("rawtypes")
  public static class PageRankUpdatePiece
      extends Piece<WritableComparable, DoubleWritable, Writable, DoubleWritable, Object> {
    @Override
    public VertexSender<WritableComparable, DoubleWritable, Writable>
    getVertexSender(BlockWorkerSendApi<WritableComparable, DoubleWritable, Writable,
          DoubleWritable> workerApi,
        Object executionStage) {
      DoubleWritable message = new DoubleWritable();
      return (vertex) -> {
        message.set(vertex.getValue().get() / vertex.getNumEdges());
        workerApi.sendMessageToAllEdges(vertex, message);
      };
    }

    @Override
    public VertexReceiver<WritableComparable, DoubleWritable, Writable, DoubleWritable>
    getVertexReceiver(BlockWorkerReceiveApi<WritableComparable> workerApi, Object executionStage) {
      return (vertex, messages) -> {
        double sum = 0;
        for (DoubleWritable value : messages) {
          sum += value.get();
        }
        vertex.getValue().set(0.15f + 0.85f * sum);
      };
    }

    @Override
    public MessageCombiner<? super WritableComparable, DoubleWritable> getMessageCombiner(
        ImmutableClassesGiraphConfiguration conf) {
      return SumMessageCombiner.DOUBLE;
    }
  }
}
