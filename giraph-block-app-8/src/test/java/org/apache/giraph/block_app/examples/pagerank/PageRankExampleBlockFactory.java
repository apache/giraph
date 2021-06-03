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
import org.apache.giraph.block_app.framework.block.RepeatBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * PageRank example of using BlockFactory - in it's simplest form, using functional primitives.
 */
public class PageRankExampleBlockFactory extends AbstractPageRankExampleBlockFactory {
  @Override
  @SuppressWarnings("rawtypes")
  public Block createBlock(GiraphConfiguration conf) {
    Block iter = Pieces.<WritableComparable, DoubleWritable, Writable, DoubleWritable>
      sendMessageToNeighbors(
        "IterationPiece",
        SumMessageCombiner.DOUBLE,
        (vertex) -> new DoubleWritable(vertex.getValue().get() / vertex.getNumEdges()),
        (vertex, value) -> {
          double sum = value != null ? value.get() : 0;
          vertex.getValue().set(0.15f + 0.85f * sum);
        });
    return new RepeatBlock(NUM_ITERATIONS.get(conf), iter);
  }
}
