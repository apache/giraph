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


/**
 * Sends vertex information to community center.
 */
public class CollectCommunityInfo extends
    Piece<LongWritable, VertexData, NullWritable, VertexInfo, Integer> {

  /**
   * Storage for received vertex information
   */
  private ObjectTransfer<Iterable<VertexInfo>> messageTransfer;

  /**
   * Piece that collects information about all vertices
   * within the community.
   * @param messageTransfer storage for received vertex information.
   */
  public CollectCommunityInfo(
      ObjectTransfer<Iterable<VertexInfo>> messageTransfer) {
    this.messageTransfer = messageTransfer;
  }

  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable,
          VertexData, NullWritable, VertexInfo> workerApi,
      Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {
      private LongWritable reusableId = new LongWritable();
      private VertexInfo message = new VertexInfo();
      @Override
      public void vertexSend(Vertex<LongWritable,
          VertexData, NullWritable> vertex) {
        reusableId.set(vertex.getValue().getCommunityId());
        message.setDegree(vertex.getValue().getDesiredDegree());
        message.setCc(vertex.getValue().getDesiredCC());
        message.setId(vertex.getId().get());
        workerApi.sendMessage(reusableId, message);
      }
    };
  }

  @Override
  public VertexReceiver<LongWritable, VertexData,
      NullWritable, VertexInfo> getVertexReceiver(
      final BlockWorkerReceiveApi<LongWritable> workerApi,
      Integer executionStage) {
    return new VertexReceiver<LongWritable, VertexData,
        NullWritable, VertexInfo>() {
      @Override
      public void vertexReceive(
          Vertex<LongWritable, VertexData, NullWritable> vertex,
          Iterable<VertexInfo> iterable) {
        messageTransfer.apply(iterable);
      }
    };
  }

  @Override
  protected Class<VertexInfo> getMessageClass() {
    return VertexInfo.class;
  }
}
