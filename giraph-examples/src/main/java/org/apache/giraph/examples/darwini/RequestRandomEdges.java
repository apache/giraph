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
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 * Creates remaining edges using Chung-Lu model.
 * This is random stage. Edges are being created
 * between random pairs of vertices without any restrictions.
 */
public class RequestRandomEdges extends
    Piece<LongWritable, VertexData,
        NullWritable, RandomEdgeRequest, Integer> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(RequestRandomEdges.class);

  /**
   * Storage for received vertex ids
   */
  private ObjectTransfer<LongWritable> target;
  /**
   * Reducer that keeps track of number of edges requested.
   */
  private ReducerHandle<LongWritable, LongWritable> totalEdgesRequested;

  /**
   * Constructor.
   * @param target storage for received vertex ids.
   */
  public RequestRandomEdges(ObjectTransfer<LongWritable> target) {
    this.target = target;
  }

  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable, VertexData,
          NullWritable, RandomEdgeRequest> workerApi,
      Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {
      private GeneratorUtils distributions =
          new GeneratorUtils(workerApi.getConf());
      private long total =
          Constants.AGGREGATE_VERTICES.get(workerApi.getConf());
      private int messages =
          Constants.RANDOM_EDGE_REQUESTS_PER_SUPERSTEP.get(
              workerApi.getConf());
      private LongWritable reusableLong = new LongWritable();
      private RandomEdgeRequest reusableMessage = new RandomEdgeRequest();
      @Override
      public void vertexSend(
          Vertex<LongWritable, VertexData, NullWritable> vertex) {
        int n = vertex.getNumEdges();
        int ndesired = vertex.getValue().getDesiredDegree();
        int edgeDemand =
            vertex.getValue().getDesiredInSuperCommunityDegree() - n;
        if (edgeDemand > 0) {
          reusableMessage.setId(vertex.getId().get());
          reusableMessage.setEdgeDemand(edgeDemand);
          reusableMessage.setDesiredDegree(ndesired);
          int cnt = 0;
          for (int i = 0; i < Math.min(messages, edgeDemand); i++) {
            long id =
                distributions.randomVertex(total);
            reusableLong.set(id);
            if (id != vertex.getId().get() &&
                vertex.getEdgeValue(reusableLong) == null) {
              //It's highly unlikely to fail for real graph. But just in case.
              cnt++;
              workerApi.sendMessage(reusableLong, reusableMessage);
            }
          }
          reusableLong.set(cnt);
          totalEdgesRequested.reduce(reusableLong);
        }
      }
    };
  }

  @Override
  public void masterCompute(BlockMasterApi masterApi, Integer executionStage) {
    LOG.info("Requested edges " +
        totalEdgesRequested.getReducedValue(masterApi));
  }

  @Override
  public VertexReceiver<LongWritable, VertexData,
      NullWritable, RandomEdgeRequest> getVertexReceiver(
      BlockWorkerReceiveApi<LongWritable> workerApi,
      Integer executionStage) {
    return new VertexReceiver<LongWritable, VertexData,
        NullWritable, RandomEdgeRequest>() {
      @Override
      public void vertexReceive(
          Vertex<LongWritable, VertexData, NullWritable> vertex,
          Iterable<RandomEdgeRequest> iterable) {
        int edgeDemand =
            vertex.getValue().getDesiredInSuperCommunityDegree() -
                vertex.getNumEdges();
        if (edgeDemand <= 0) {
          return; //This vertex has enough, ignore all incomming requests
        }
        long targetId = -1;
        double biggestNeed = -1;
        for (RandomEdgeRequest request: iterable) {
          double rnd = Math.random();
          if (rnd > biggestNeed) {
            biggestNeed = request.getEdgeDemand();
            targetId = request.getId();
          }
        }
        if (biggestNeed > 0) {
          target.apply(new LongWritable(targetId));
        }
      }
    };
  }

  @Override
  protected Class<RandomEdgeRequest> getMessageClass() {
    return RandomEdgeRequest.class;
  }

  @Override
  public void registerReducers(CreateReducersApi reduceApi,
                               Integer executionStage) {
    totalEdgesRequested = reduceApi.createLocalReducer(SumReduce.LONG);
  }
}
