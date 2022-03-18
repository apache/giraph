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

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Build communities by grouping vertices with the
 * same value of cc * degree * (degree - 1) together.
 */
public class BuildCommunities extends
    Piece<LongWritable, VertexData, NullWritable, NoMessage, Integer> {

  /**
   * Reducers that aggregate information about all vertices.
   */
  private ReducerHandle<VertexInfo, WritableVertexRequests>[] reduceVertices;
  /**
   * Broadcasters that send information about all vertices back.
   */
  private BroadcastHandle<WritableIntArray>[] broadcast;

  @Override
  public void registerReducers(CreateReducersApi reduceApi,
                               Integer executionStage) {
    int splits = Constants.AGGREGATORS_SPLITS.get(reduceApi.getConf());
    if (Constants.AGGREGATE_VERTICES.get(reduceApi.getConf()) >=
        splits * (long) Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Sorry, can't optimize " +
          "globaly for that many vertices");
    }

    reduceVertices = new ReducerHandle[splits];
    for (int i = 0; i < splits; i++) {
      reduceVertices[i] = reduceApi.createGlobalReducer(
          new ReduceWritableArrayOperation(
              (int) (Constants.AGGREGATE_VERTICES.get(
                  reduceApi.getConf()) / splits)));
    }
  }

  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable, VertexData,
          NullWritable, NoMessage> workerApi,
      Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {
      private VertexInfo reusable = new VertexInfo();
      private int splits =
          Constants.AGGREGATORS_SPLITS.get(workerApi.getConf());
      private long cnt =
          Constants.AGGREGATE_VERTICES.get(workerApi.getConf());
      private int splitSize = (int) (cnt / splits);

      @Override
      public void vertexSend(Vertex<LongWritable,
          VertexData, NullWritable> vertex) {
        reusable.setDegree(vertex.getValue().getDesiredDegree());
        reusable.setCc(vertex.getValue().getDesiredCC());
        reusable.setId(vertex.getId().get());
        reduceVertices[(int) (vertex.getId().get() /
            splitSize)].reduce(reusable);
      }
    };
  }

  @Override
  public VertexReceiver<LongWritable, VertexData,
      NullWritable, NoMessage> getVertexReceiver(
      final BlockWorkerReceiveApi<LongWritable> workerApi,
      Integer executionStage) {
    return new VertexReceiver<LongWritable, VertexData,
        NullWritable, NoMessage>() {
      private int splits =
          Constants.AGGREGATORS_SPLITS.get(workerApi.getConf());
      private long cnt =
          Constants.AGGREGATE_VERTICES.get(workerApi.getConf());
      private int splitSize = (int) (cnt / splits);

      @Override
      public void vertexReceive(
          Vertex<LongWritable, VertexData, NullWritable> vertex,
          Iterable<NoMessage> iterable) {
        WritableIntArray distribution =
            broadcast[(int) (vertex.getId().get() /
                splitSize)].getBroadcast(workerApi);
        vertex.getValue().setCommunityId(
            distribution.getData()[(int) (vertex.getId().get() % splitSize)]);
      }
    };
  }

  @Override
  public void masterCompute(BlockMasterApi masterApi, Integer executionStage) {
    Long2ObjectAVLTreeMap<Community> communityMap =
        new Long2ObjectAVLTreeMap<>();
    GeneratorUtils distributions = new GeneratorUtils(masterApi.getConf());
    int splits = Constants.AGGREGATORS_SPLITS.get(masterApi.getConf());
    long totalVertices =
        Constants.AGGREGATE_VERTICES.get(masterApi.getConf());
    int splitSize = (int) (totalVertices / splits);
    int[][] communities = new int[splits][splitSize];
    WritableVertexRequests[] infos = new WritableVertexRequests[splits];

    for (int j = 0; j < reduceVertices.length; j++) {
      infos[j] = reduceVertices[j].getReducedValue(masterApi);

      for (int i = 0; i < splitSize; i++) {
        int degree = infos[j].getDegree(i);
        if (degree > 0) {

          long hash = distributions.hash(degree, infos[j].getCC(i));
          Community comm = communityMap.get(hash);
          if (comm == null) {
            comm = new Community();
            communityMap.put(hash, comm);
          }
          if (comm.count == 0) {
            comm.id = j * splitSize + i;
            comm.expectedCnt = degree + 1;
          }
          comm.expectedCnt = Math.min(comm.expectedCnt, degree + 1);
          comm.count++;
          if (comm.count >= comm.expectedCnt) {
            comm.count = 0;
          }
          communities[j][i] = (int) comm.id;
        }
      }
    }
    Long2LongOpenHashMap remap = new Long2LongOpenHashMap();
    Community current = null;
    for (long hash : communityMap.keySet()) { //sorted by cc expectations
      Community community = communityMap.get(hash);
      if (community.count == 0 || community.count > hash) {
        continue;
      }
      if (current == null) {
        current = community;
      } else {
        remap.put(community.id, current.id);
        current.count += community.count;
        current.expectedCnt =
            Math.min(current.expectedCnt, community.expectedCnt);
      }

      if (current.count >= current.expectedCnt) {
        current = null;
      }
    }
    for (int j = 0; j < splits; j++) {
      for (int i = 0; i < splitSize; i++) {
        if (remap.containsKey(communities[j][i])) {
          communities[j][i] = (int) remap.get(communities[j][i]);
        }
      }
    }
    broadcast = new BroadcastHandle[splits];
    for (int i = 0; i < splits; i++) {
      broadcast[i] = masterApi.broadcast(
          new WritableIntArray(communities[i]));
    }
  }

  /**
   * Community structure
   */
  private static class Community {
    /**
     * Community id
     */
    private long id;
    /**
     * Current community size
     */
    private int count;
    /**
     * Expected size of the community.
     */
    private int expectedCnt;
  }
}
