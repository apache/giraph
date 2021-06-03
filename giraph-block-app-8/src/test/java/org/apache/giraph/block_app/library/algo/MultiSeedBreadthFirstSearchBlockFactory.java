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
package org.apache.giraph.block_app.library.algo;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.reducers.TopNReduce;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.function.TripleFunction;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.writable.kryo.TransientRandom;
import org.apache.hadoop.io.*;

import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Example Application of BFS calculation with multiple seeds
 */
public class MultiSeedBreadthFirstSearchBlockFactory
  extends AbstractBlockFactory<Object> {
  public static final IntConfOption RANDOM_SEED_COUNT =
    new IntConfOption("multi_seed_bfs.random_seeds", 0,
      "If using random seeds, the count of random seeds to be generated");
  public static final StrConfOption SEED_LIST =
    new StrConfOption("multi_seed_bfs.seed_list", "",
      "List of comma separated IDs of the seed vertices");

  private static
  TripleFunction<Vertex<LongWritable,
    MultiSeedBreadthFirstSearchVertexValue, Writable>, IntWritable,
    Iterator<IntWritable>, IntWritable> traverseVertex() {
    IntWritable notReachableVertex = new IntWritable(-1);
    IntWritable vertexValue = new IntWritable();
    IntWritable reservoirValue = new IntWritable();
    TransientRandom random = new TransientRandom();
    IntWritable reusableMessage = new IntWritable();
    // Reservoir sampling to select the seed from the set of messages received
    return (vertex, distance, messageIter) -> {
      vertexValue.set(vertex.getValue().getDistance());
      if (vertexValue.compareTo(notReachableVertex) == 0 ||
        vertexValue.compareTo(distance) > 0) {
        reservoirValue.set(messageIter.next().get());
        int messageIndex = 1;
        while (messageIter.hasNext()) {
          if (random.nextInt(messageIndex + 1) < 1) {
            reservoirValue.set(messageIter.next().get());
          } else {
            messageIter.next();
          }
          messageIndex++;
        }
        vertex.getValue().setSourceIndex(reservoirValue.get());
        vertex.getValue().setDistance(distance.get());
        reusableMessage.set(vertex.getValue().getSourceIndex());
        return reusableMessage;
      } else {
        return null;
      }
    };
  }

  @Override public Block createBlock(GiraphConfiguration conf) {
    Long2IntOpenHashMap seeds = new Long2IntOpenHashMap();
    Piece pickSeedVertices = null;
    if (RANDOM_SEED_COUNT.get(conf) > 0) {
      TransientRandom random = new TransientRandom();
      pickSeedVertices = Pieces.reduce("SeedSelection",
        new TopNReduce<VertexLongPair<LongWritable>>(
          RANDOM_SEED_COUNT.get(conf)), (vertex) -> {
          return new VertexLongPair<LongWritable>((LongWritable) vertex.getId(),
            random.get().nextLong());
        }, (result) -> {
          PriorityQueue<VertexLongPair<LongWritable>> queue = result.get();
          int index = 0;
          while (!queue.isEmpty()) {
            VertexLongPair<LongWritable> nextPair = queue.poll();
            seeds.put(nextPair.getVertex().get(), index++);
          }
        });
    } else {
      String[] sepStr = SEED_LIST.get(conf).split(", ");
      for (int ii = 0; ii < sepStr.length; ++ii) {
        seeds.put(Long.parseLong(sepStr[ii]), ii);
      }
    }

    IntWritable reusableMessage = new IntWritable();
    SupplierFromVertex<LongWritable, MultiSeedBreadthFirstSearchVertexValue,
          Writable, IntWritable>
      initializeVertex = (vertex) -> {
      if (seeds.containsKey(vertex.getId().get())) {
        vertex.getValue().setDistance(0);
        vertex.getValue().setSourceIndex(seeds.get(vertex.getId().get()));
        reusableMessage.set(vertex.getValue().getSourceIndex());
        return reusableMessage;
      } else {
        vertex.getValue().setDistance(-1);
        vertex.getValue().setSourceIndex(-1);
        return null;
      }
    };

    if (RANDOM_SEED_COUNT.get(conf) > 0) {
      return new SequenceBlock(pickSeedVertices, BreadthFirstSearch
        .bfs(IntWritable.class, initializeVertex, traverseVertex()));
    } else {
      return BreadthFirstSearch
        .bfs(IntWritable.class, initializeVertex, traverseVertex());
    }
  }

  @Override public Object createExecutionStage(GiraphConfiguration conf) {
    return new Object();
  }

  @Override protected Class<LongWritable> getVertexIDClass(
    GiraphConfiguration conf) {
    return LongWritable.class;
  }

  @Override protected Class<MultiSeedBreadthFirstSearchVertexValue>
  getVertexValueClass(
    GiraphConfiguration conf) {
    return MultiSeedBreadthFirstSearchVertexValue.class;
  }

  @Override protected Class<NullWritable> getEdgeValueClass(
    GiraphConfiguration conf) {
    return NullWritable.class;
  }
}

