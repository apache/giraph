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

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *  PageRank example with convergence check, using functional primitives
 */
public class PageRankWithTransferAndConvergenceExampleBlockFactory
    extends AbstractPageRankExampleBlockFactory {
  private static final double EPS = 1e-3;
  private static final LongWritable ONE = new LongWritable(1);
  private static final LongWritable ZERO = new LongWritable(0);

  @Override
  @SuppressWarnings("rawtypes")
  public Block createBlock(GiraphConfiguration conf) {
    ObjectTransfer<Boolean> converged = new ObjectTransfer<>();
    ObjectTransfer<Double> vertexValueChange = new ObjectTransfer<>();

    Block iter = Pieces.<WritableComparable, DoubleWritable, Writable, DoubleWritable>
    sendMessageToNeighbors(
        "IterationPiece",
        SumMessageCombiner.DOUBLE,
        (vertex) -> new DoubleWritable(vertex.getValue().get() / vertex.getNumEdges()),
        (vertex, value) -> {
          double sum = value != null ? value.get() : 0;
          double newValue = 0.15f + 0.85f * sum;
          vertexValueChange.apply(Math.abs(newValue - vertex.getValue().get()));
          vertex.getValue().set(newValue);
        });

    Block checkConverged = Pieces.reduce(
        "CheckConvergedPiece",
        SumReduce.LONG,
        (vertex) -> {
          double change = vertexValueChange.get();
          return (change > EPS) ? ONE : ZERO;
        },
        (changingCount) -> converged.apply(changingCount.get() == 0));

    return new RepeatUntilBlock(
        NUM_ITERATIONS.get(conf),
        new SequenceBlock(iter, checkConverged),
        converged);
  }
}
