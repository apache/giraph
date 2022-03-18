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

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Random;

/**
 * Create community edges.
 */
public class CreateCommunityEdges extends
    Piece<LongWritable, VertexData, NullWritable, NoMessage, Integer> {

  /**
   * Transfers messages from previous piece.
   */
  private final ObjectTransfer<Iterable<VertexInfo>> messageTransfer;

  /**
   * Constructor that receives messages transfer object
   * @param messageTransfer transfers messages from previous piece
   */
  public CreateCommunityEdges(
      ObjectTransfer<Iterable<VertexInfo>> messageTransfer) {
    this.messageTransfer = messageTransfer;
  }

  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable, VertexData,
          NullWritable, NoMessage> workerApi,
      Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {

      private Random rnd = new Random();

      @Override
      public void vertexSend(
          Vertex<LongWritable, VertexData, NullWritable> vertex) {
        Iterable<VertexInfo> messages = messageTransfer.get();
        int degree = Integer.MAX_VALUE;
        double cc = 0;
        int cnt = 0;
        for (VertexInfo message : messages) {
          cnt++;
          if (message.getDegree() < degree) {
            degree = message.getDegree();
            cc = message.getCc();
          }
        }
        double connectivity =
            Math.pow(cc * degree * (degree - 1.) /
                ((cnt - 1.) * (cnt - 2.)), 1. / 3);
        for (VertexInfo message : messages) {
          long id1 = message.getId();
          for (VertexInfo message2 : messages) {
            long id2 = message2.getId();
            if (id1 < id2 && rnd.nextDouble() <= connectivity) {
              workerApi.addEdgeRequest(new LongWritable(id1),
                  EdgeFactory.create(new LongWritable(id2)));
              workerApi.addEdgeRequest(new LongWritable(id2),
                  EdgeFactory.create(new LongWritable(id1)));
            }
          }
        }
      }
    };
  }
}

