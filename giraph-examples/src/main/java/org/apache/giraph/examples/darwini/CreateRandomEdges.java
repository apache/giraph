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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 * Create random edges.
 */
public class CreateRandomEdges extends
    Piece<LongWritable, VertexData, NullWritable, BooleanWritable, Integer> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(CreateRandomEdges.class);

  /**
   * Source vertices that requested edges
   */
  private ObjectTransfer<LongWritable> targetTransfer;
  /**
   * Halt condition
   */
  private ObjectTransfer<Boolean> randomEdgesHalt;
  /**
   * Number of edges added
   */
  private ReducerHandle<LongWritable, LongWritable> edgesAdded;
  /**
   * Number of edges still missing.
   */
  private ReducerHandle<LongWritable, LongWritable> totalEdgesRequired;
  /**
   * Total number of edges reducer.
   */
  private ReducerHandle<LongWritable, LongWritable> totalEdges;
  /**
   * Total number of vertices reducer.
   */
  private ReducerHandle<LongWritable, LongWritable> totalVertices;

  /**
   * Constructs this piece with two object transfers:
   * first to pass around counterparty id and the second one
   * to be able to halt computations when we can't create any
   * more vertices.
   * @param target holds an id of the vertex that requested and edge
   * @param randomEdgesHalt stop condition
   */
  public CreateRandomEdges(ObjectTransfer<LongWritable> target,
                           ObjectTransfer<Boolean> randomEdgesHalt) {
    this.targetTransfer = target;
    this.randomEdgesHalt = randomEdgesHalt;
  }

  @Override
  public VertexSender<LongWritable, VertexData, NullWritable> getVertexSender(
      final BlockWorkerSendApi<LongWritable, VertexData,
          NullWritable, BooleanWritable> workerApi,
      Integer executionStage) {
    return new VertexSender<LongWritable, VertexData, NullWritable>() {
      private LongWritable reusableLong = new LongWritable();
      private BooleanWritable reusableMessage = new BooleanWritable();
      @Override
      public void vertexSend(Vertex<LongWritable,
          VertexData, NullWritable> vertex) {
        reusableLong.set(1);
        totalVertices.reduce(reusableLong);
        reusableLong.set(vertex.getNumEdges());
        totalEdges.reduce(reusableLong);

        LongWritable target = targetTransfer.get();
        int remainingDegree =
            vertex.getValue().getDesiredInSuperCommunityDegree() -
            vertex.getNumEdges();
        if (remainingDegree > 0) {
          reusableLong.set(remainingDegree);
          totalEdgesRequired.reduce(reusableLong);
          if (target != null) {
            workerApi.addEdgeRequest(vertex.getId(),
                EdgeFactory.create(target));
            workerApi.addEdgeRequest(target,
                EdgeFactory.create(vertex.getId()));
            workerApi.sendMessage(target, reusableMessage);
            reusableLong.set(2);
            edgesAdded.reduce(reusableLong);
          }
        }
      }
    };
  }

  @Override
  public void masterCompute(BlockMasterApi masterApi,
                            Integer executionStage) {
    long addedEdges = edgesAdded.getReducedValue(masterApi).get();
    LOG.info("Added edges: " + addedEdges);
    LOG.info("Required edges: " +
        totalEdgesRequired.getReducedValue(masterApi).get());
    LOG.info("Average edges per vertex: " +
        totalEdges.getReducedValue(masterApi).get() /
            totalVertices.getReducedValue(masterApi).get());
    randomEdgesHalt.apply(addedEdges == 0);
  }

  @Override
  public void registerReducers(CreateReducersApi reduceApi,
                               Integer executionStage) {
    edgesAdded = reduceApi.createLocalReducer(SumReduce.LONG);
    totalEdgesRequired = reduceApi.createLocalReducer(SumReduce.LONG);
    totalEdges = reduceApi.createLocalReducer(SumReduce.LONG);
    totalVertices = reduceApi.createLocalReducer(SumReduce.LONG);
  }

  @Override
  protected Class<BooleanWritable> getMessageClass() {
    return BooleanWritable.class;
  }
}
