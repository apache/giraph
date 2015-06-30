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
package org.apache.giraph.block_app.library.stats;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.map.ReducerMapHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.block_app.reducers.map.BasicMapReduce;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.impl.MaxReduce;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;

/** Utility class for calculating stats of a directed graph */
public class DirectedGraphStats {
  private static final Logger LOG = Logger.getLogger(DirectedGraphStats.class);
  private static final double LOG_2 = Math.log(2);
  private static final int MAX_LOG_DEGREE = 20;

  private DirectedGraphStats() { }

  /**
   * Calculate and print on master statistics about in and out degrees
   * of all vertices.
   */
  public static <I extends WritableComparable>
  Block createInAndOutDegreeStatsBlock(Class<I> idClass) {
    ObjectTransfer<Iterable<I>> inEdges = new ObjectTransfer<>();

    Block announceToNeighbors = Pieces.sendMessageToNeighbors(
        "AnnounceToNeighbors",
        idClass,
        VertexSuppliers.<I, Writable, Writable>vertexIdSupplier(),
        inEdges.<I, Writable, Writable>castToConsumer());

    return new SequenceBlock(
        announceToNeighbors,
        new AggregateInAndOutDegreeStatsPiece<>(
            inEdges.<I, Writable, Writable>castToSupplier()));
  }

  /** Aggregating in and out degree statistics */
  private static class AggregateInAndOutDegreeStatsPiece
      <I extends WritableComparable>
      extends Piece<I, Writable, Writable, Writable, Object> {
    private final
    SupplierFromVertex<I, Writable, Writable, Iterable<I>> inEdges;

    private ReducerHandle<IntWritable, IntWritable> maxDegreeAgg;

    private ReducerMapHandle<IntWritable, LongWritable, LongWritable>
    inHistograms;
    private ReducerMapHandle<IntWritable, LongWritable, LongWritable>
    outHistograms;

    private ReducerMapHandle<IntWritable, LongWritable, LongWritable>
    inVsOutHistograms;


    public AggregateInAndOutDegreeStatsPiece(
        SupplierFromVertex<I, Writable, Writable, Iterable<I>> inEdges) {
      this.inEdges = inEdges;
    }

    @Override
    public void registerReducers(
        CreateReducersApi reduceApi, Object executionStage) {
      inHistograms = BasicMapReduce.createLocalMapHandles(
        IntTypeOps.INSTANCE, LongTypeOps.INSTANCE, SumReduce.LONG, reduceApi);
      outHistograms = BasicMapReduce.createLocalMapHandles(
        IntTypeOps.INSTANCE, LongTypeOps.INSTANCE, SumReduce.LONG, reduceApi);

      inVsOutHistograms = BasicMapReduce.createLocalMapHandles(
        IntTypeOps.INSTANCE, LongTypeOps.INSTANCE, SumReduce.LONG, reduceApi);

      maxDegreeAgg =
          reduceApi.createLocalReducer(new MaxReduce<>(IntTypeOps.INSTANCE));
    }

    @Override
    public VertexSender<I, Writable, Writable> getVertexSender(
        BlockWorkerSendApi<I, Writable, Writable, Writable> workerApi,
        Object executionStage) {
      final IntWritable indexWrap = new IntWritable();

      return new InnerVertexSender() {
        @Override
        public void vertexSend(Vertex<I, Writable, Writable> vertex) {
          Iterable<I> in = inEdges.get(vertex);

          int inCount = Iterables.size(in);
          int outCount = vertex.getNumEdges();

          reduceInt(maxDegreeAgg, Math.max(inCount, outCount));
          increment(inHistograms, inCount);
          increment(outHistograms, outCount);
          increment(inVsOutHistograms,
              log2(inCount + 1) * MAX_LOG_DEGREE + log2(outCount + 1));

          // TODO add count for common edges.
        }

        private int log2(int value) {
          return (int) (Math.log(value) / LOG_2);
        }

        private void increment(
            ReducerMapHandle<IntWritable, LongWritable, LongWritable>
              reduceHandle,
            int index) {
          indexWrap.set(index);
          reduceLong(inHistograms.get(indexWrap), 1);
        }
      };
    }

    @Override
    public void masterCompute(BlockMasterApi master, Object executionStage) {

      int maxDegree = maxDegreeAgg.getReducedValue(master).get();
      LOG.info("Max degree : " + maxDegree);

      StringBuilder sb = new StringBuilder("In and out degree histogram:\n");
      sb.append("degree\tnumIn\tnumOut\n");

      final IntWritable index = new IntWritable();

      for (int i = 0; i <= maxDegree; i++) {
        index.set(i);
        long numIn = inHistograms.get(index).getReducedValue(master).get();
        long numOut = outHistograms.get(index).getReducedValue(master).get();
        if (numIn > 0 || numOut > 0) {
          sb.append(i + "\t" + numIn + "\t" + numOut + "\n");
        }
      }
      LOG.info(sb);

      sb = new StringBuilder("In vs out degree log/log histogram:\n");
      sb.append("<inDeg\t<outDeg\tnum\n");

      for (int in = 0; in < MAX_LOG_DEGREE; in++) {
        for (int out = 0; out < MAX_LOG_DEGREE; out++) {
          index.set(in * MAX_LOG_DEGREE + out);
          long num = inVsOutHistograms.get(index).getReducedValue(master).get();
          if (num > 0) {
            sb.append(Math.pow(2, in) + "\t" + Math.pow(2, out) +
                "\t" + num + "\n");
          }
        }
      }
      LOG.info(sb);
    }
  }
}
