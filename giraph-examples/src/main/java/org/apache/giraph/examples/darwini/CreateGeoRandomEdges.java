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
package org.apache.giraph.examples.darwini;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Random;

/**
 * Creates random edge between communities.
 */
public class CreateGeoRandomEdges extends
    Piece<LongWritable, VertexData, NullWritable, NoMessage, Integer> {

  /**
   * Logger
   */
  private static final Logger LOG =
      Logger.getLogger(CreateGeoRandomEdges.class);

  /**
   * Edge requests received in previous piece.
   */
  private ObjectTransfer<Iterable<RandomEdgeRequest>> messageTransfer;
  /**
   * Number of edge we've attemped to create
   */
  private ReducerHandle<LongWritable, LongWritable> attempedToCreate;

  /**
   * Constructs this piece with an object transfer to pass messages
   * from the previous piece.
   * @param messageTransfer messages received in previous piece.
   */
  public CreateGeoRandomEdges(
      ObjectTransfer<Iterable<RandomEdgeRequest>> messageTransfer) {
    this.messageTransfer = messageTransfer;
  }

  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable, VertexData,
          NullWritable, NoMessage> workerApi,
      Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {
      private Random rnd = new Random();
      private LongList ids = new LongArrayList();
      private IntList degrees = new IntArrayList();
      private DoubleList edgesCounts = new DoubleArrayList();
      @Override
      public void vertexSend(Vertex<LongWritable,
          VertexData, NullWritable> unused) {
        Iterable<RandomEdgeRequest> messages = messageTransfer.get();
        if (messages != null) {
          ids.clear();
          edgesCounts.clear();
          degrees.clear();
          long totaleEdgesRequested = 0;
          for (RandomEdgeRequest rq : messages) {
            totaleEdgesRequested += rq.getEdgeDemand();
            degrees.add(rq.getDesiredDegree());
            ids.add(rq.getId());
          }
          if (degrees.size() < 2) {
            return;
          }

          double totalDemand = 0;
          for (RandomEdgeRequest rq : messages) {
            totalDemand += rq.getEdgeDemand() / (1. * totaleEdgesRequested);
            edgesCounts.add(totalDemand);
          }

          for (int i = 0; i < totaleEdgesRequested; i++) {
            double v1 = rnd.nextDouble();
            int v1p = Collections.binarySearch(edgesCounts, v1);
            if (v1p < 0) {
              v1p = -1 - v1p;
            }
            long v1id = ids.getLong(v1p);
            int v2p;
            do {
              double v2 = rnd.nextDouble();
              v2p = Collections.binarySearch(edgesCounts, v2);
              if (v2p < 0) {
                v2p = -1 - v2p;
              }
            } while (v1p == v2p);

            long v2id = ids.getLong(v2p);
            int d1 = degrees.getInt(v1p);
            int d2 = degrees.getInt(v2p);

            double degreeDiff = GeneratorUtils.scoreDegreeDiff(d1, d2);
            if (rnd.nextDouble() > degreeDiff) {
              workerApi.addEdgeRequest(new LongWritable(v1id),
                  EdgeFactory.create(new LongWritable(v2id)));
              workerApi.addEdgeRequest(new LongWritable(v2id),
                  EdgeFactory.create(new LongWritable(v1id)));
            }
          }
        }
      }
    };
  }

  @Override
  public void masterCompute(BlockMasterApi masterApi,
                            Integer executionStage) {
    LOG.info("Geo random edges attempt " +
        attempedToCreate.getReducedValue(masterApi));
  }

  @Override
  public void registerReducers(CreateReducersApi reduceApi,
                               Integer executionStage) {
    attempedToCreate = reduceApi.createLocalReducer(SumReduce.LONG);
  }

}
