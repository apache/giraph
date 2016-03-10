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

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Random;

/**
 * Repeatedly groups vertices into ever-increasing
 * communities and then creates edges within those communities
 * in a way that satisfies joint degree distribution.
 */
public class RequestGeoRandomEdges extends
    Piece<LongWritable, VertexData, NullWritable, RandomEdgeRequest, Integer> {

  /**
   * Storage to pass messages to the next piece
   */
  private ObjectTransfer<Iterable<RandomEdgeRequest>> messageTransfer;

  /**
   * Constructor that has access to message storage.
   * @param messageTransfer to pass messages around to the next piece
   */
  public RequestGeoRandomEdges(
      ObjectTransfer<Iterable<RandomEdgeRequest>> messageTransfer) {
    this.messageTransfer = messageTransfer;
  }


  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable,
          VertexData, NullWritable, RandomEdgeRequest> workerApi,
      final Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {
      private RandomEdgeRequest reusableMessage = new RandomEdgeRequest();
      private LongWritable reusableId = new LongWritable();
      private long totalVertices =
          Constants.AGGREGATE_VERTICES.get(workerApi.getConf());
      private float edgeDemandRatio =
          Constants.GEO_RATIO.get(workerApi.getConf());
      private Random rnd = new Random();

      @Override
      public void vertexSend(
          Vertex<LongWritable, VertexData, NullWritable> vertex) {
        int ndesired = vertex.getValue().getDesiredInSuperCommunityDegree();
        int n = vertex.getNumEdges();
        if (n >= ndesired) {
          return;
        }
        int edgeDemand = ndesired - n;

        if (edgeDemand > 0) {
          long randomVertex = (long) (totalVertices * rnd.nextDouble());
          long central = (randomVertex >>> executionStage) << executionStage;
          reusableId.set(central);
          reusableMessage.setDesiredDegree(
              vertex.getValue().getDesiredDegree());
          reusableMessage.setEdgeDemand(
              (int) (1 + edgeDemandRatio *
                  rnd.nextDouble() * (edgeDemand - 1)));
          reusableMessage.setId(vertex.getId().get());
          workerApi.sendMessage(reusableId, reusableMessage);
        }
      }
    };
  }

  @Override
  public VertexReceiver<LongWritable, VertexData,
      NullWritable, RandomEdgeRequest> getVertexReceiver(
      BlockWorkerReceiveApi<LongWritable> workerApi, Integer executionStage) {
    return new VertexReceiver<LongWritable, VertexData,
        NullWritable, RandomEdgeRequest>() {
      @Override
      public void vertexReceive(
          Vertex<LongWritable, VertexData, NullWritable> vertex,
          Iterable<RandomEdgeRequest> iterable) {
        messageTransfer.apply(iterable);
      }
    };
  }


  @Override
  protected Class<RandomEdgeRequest> getMessageClass() {
    return RandomEdgeRequest.class;
  }
}
