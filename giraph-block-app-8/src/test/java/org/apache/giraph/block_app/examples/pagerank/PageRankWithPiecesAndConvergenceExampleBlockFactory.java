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

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * PageRank example with convergence check.
 */
@SuppressWarnings("rawtypes")
public class PageRankWithPiecesAndConvergenceExampleBlockFactory extends AbstractPageRankExampleBlockFactory {
  private static final double EPS = 1e-3;

  @Override
  public Block createBlock(GiraphConfiguration conf) {
    ObjectTransfer<Boolean> converged = new ObjectTransfer<>();
    ObjectTransfer<Double> vertexValueChange = new ObjectTransfer<>();
    return new RepeatUntilBlock(
      NUM_ITERATIONS.get(conf),
      new SequenceBlock(
        new PageRankUpdatePiece(vertexValueChange),
        new PageRankConvergencePiece(vertexValueChange, converged)
      ),
      converged);
  }

  /** One PageRank iteration */
  public static class PageRankUpdatePiece
      extends Piece<WritableComparable, DoubleWritable, Writable, DoubleWritable, Object> {
    private final Consumer<Double> changeConsumer;

    public PageRankUpdatePiece(Consumer<Double> changeConsumer) {
      this.changeConsumer = changeConsumer;
    }

    @Override
    public VertexSender<WritableComparable, DoubleWritable, Writable> getVertexSender(
        final BlockWorkerSendApi<WritableComparable, DoubleWritable, Writable,
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
        double newValue = 0.15f + 0.85f * sum;
        changeConsumer.apply(Math.abs(newValue - vertex.getValue().get()));
        vertex.getValue().set(newValue);
      };
    }

    @Override
    public MessageCombiner<? super WritableComparable, DoubleWritable> getMessageCombiner(
        ImmutableClassesGiraphConfiguration conf) {
      return SumMessageCombiner.DOUBLE;
    }
  }

  /** PageRank convergence check */
  public static class PageRankConvergencePiece
      extends Piece<WritableComparable, DoubleWritable, Writable, NoMessage, Object> {
    private ReducerHandle<LongWritable, LongWritable> countModified;

    private final Supplier<Double> changeSupplier;
    private final Consumer<Boolean> converged;

    public PageRankConvergencePiece(
        Supplier<Double> changeSupplier,
        Consumer<Boolean> converged) {
      this.changeSupplier = changeSupplier;
      this.converged = converged;
    }

    @Override
    public void registerReducers(CreateReducersApi reduceApi, Object executionStage) {
      countModified = reduceApi.createLocalReducer(SumReduce.LONG);
    }

    @Override
    public VertexSender<WritableComparable, DoubleWritable, Writable>
    getVertexSender(
        BlockWorkerSendApi<WritableComparable, DoubleWritable, Writable, NoMessage> workerApi,
        Object executionStage) {
      return (vertex) -> {
        double change = changeSupplier.get();
        if (change > EPS) {
          reduceLong(countModified, 1);
        }
      };
    }

    @Override
    public void masterCompute(BlockMasterApi master, Object executionStage) {
      LongWritable count = countModified.getReducedValue(master);
      converged.apply(count.get() == 0);
    }
  }
}
