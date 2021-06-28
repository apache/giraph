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

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Creates edges between communities during the merge stage.
 */
public class MergeCreateGeoRandomEdges extends
    Piece<LongWritable, VertexData, NullWritable, NoMessage, Integer> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(
      MergeCreateGeoRandomEdges.class);

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
   *
   * @param messageTransfer messages received in previous piece.
   */
  public MergeCreateGeoRandomEdges(
      ObjectTransfer<Iterable<RandomEdgeRequest>> messageTransfer) {
    this.messageTransfer = messageTransfer;
  }

  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable, VertexData,
          NullWritable, NoMessage> workerApi,
      Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {
      private SortedSet<RandomEdgeRequest> requests =
        new TreeSet<RandomEdgeRequest>(
          new Comparator<RandomEdgeRequest>() {
            @Override
            public int compare(RandomEdgeRequest r1,
                               RandomEdgeRequest r2) {
              return Long.compare(r1.getId(), r2.getId());
            }
          });
      private LongWritable reusableLong = new LongWritable(1);
      private long[] boundaries = Utils.getBoundaries(workerApi.getConf());

      @Override
      public void vertexSend(Vertex<LongWritable,
          VertexData, NullWritable> unused) {
        Iterable<RandomEdgeRequest> messages = messageTransfer.get();
        if (messages != null) {
          requests.clear();
          for (RandomEdgeRequest msg : messages) {
            requests.add(new RandomEdgeRequest(
                msg.getId(), msg.getEdgeDemand(), msg.getDesiredDegree()));
          }
          int p1 = 0;
          for (RandomEdgeRequest rq1 : requests) {
            int c1 = Utils.superCommunity(rq1.getId(), boundaries);
            int p2 = 0;
            for (RandomEdgeRequest rq2 : requests) {
              if (p2 <= p1) {
                p2++;
                continue;
              }
              int c2 = Utils.superCommunity(rq2.getId(), boundaries);
              if (c1 != c2 && rq2.getEdgeDemand() > 0 &&
                  rq1.getEdgeDemand() > 0) {
                double degreeDiff = GeneratorUtils.scoreDegreeDiff(
                    rq1.getDesiredDegree(), rq2.getDesiredDegree());
                if (Math.random() > degreeDiff) {
                  rq2.setEdgeDemand(rq2.getEdgeDemand() - 1);
                  rq1.setEdgeDemand(rq1.getEdgeDemand() - 1);
                  //Where might be an edge already, but we don't care
                  workerApi.addEdgeRequest(new LongWritable(rq1.getId()),
                      EdgeFactory.create(new LongWritable(rq2.getId())));
                  workerApi.addEdgeRequest(new LongWritable(rq2.getId()),
                      EdgeFactory.create(new LongWritable(rq1.getId())));

                  attempedToCreate.reduce(reusableLong);
                }
              }
              p2++;
            }
            p1++;
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
