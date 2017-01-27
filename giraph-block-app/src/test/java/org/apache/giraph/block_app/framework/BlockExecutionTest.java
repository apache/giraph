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
package org.apache.giraph.block_app.framework;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.primitive.PrimitiveRefs.IntRef;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Assert;
import org.junit.Test;
import org.python.google.common.collect.Iterables;

/**
 * Test of barebones of Blocks Framework.
 *
 * Do not look as an example of unit test, or to learn about the Framework,
 * there are utilities to do things simpler, that we are not trying to test
 * here.
 */
public class BlockExecutionTest {

  private static GiraphConfiguration createConf() {
    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphConstants.VERTEX_ID_CLASS.set(conf, LongWritable.class);
    GiraphConstants.VERTEX_VALUE_CLASS.set(conf, LongWritable.class);
    GiraphConstants.EDGE_VALUE_CLASS.set(conf, NullWritable.class);
    return conf;
  }

  private static TestGraph<LongWritable, LongWritable, NullWritable> createTestGraph() {
    TestGraph<LongWritable, LongWritable, NullWritable> graph =
        new TestGraph<LongWritable, LongWritable, NullWritable>(createConf());
    graph.addVertex(new LongWritable(1), new LongWritable());
    graph.addVertex(new LongWritable(2), new LongWritable());
    graph.addVertex(new LongWritable(3), new LongWritable());
    graph.addVertex(new LongWritable(4), new LongWritable());

    graph.addEdge(new LongWritable(1), new LongWritable(2), NullWritable.get());
    graph.addEdge(new LongWritable(2), new LongWritable(1), NullWritable.get());
    graph.addEdge(new LongWritable(2), new LongWritable(3), NullWritable.get());
    graph.addEdge(new LongWritable(3), new LongWritable(2), NullWritable.get());
    return graph;
  }

  @Test
  public void testMessageSending() {
    TestGraph<LongWritable, LongWritable, NullWritable> graph = createTestGraph();

    LocalBlockRunner.runBlock(graph, new Piece<WritableComparable, LongWritable, Writable, BooleanWritable, Object>() {
      @Override
      public VertexSender<WritableComparable, LongWritable, Writable> getVertexSender(
          final BlockWorkerSendApi<WritableComparable, LongWritable, Writable, BooleanWritable> workerApi,
          Object executionStage) {
        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<WritableComparable, LongWritable, Writable> vertex) {
            workerApi.sendMessageToAllEdges(vertex, new BooleanWritable());
          }
        };
      }

      @Override
      public VertexReceiver<WritableComparable, LongWritable, Writable, BooleanWritable>
          getVertexReceiver(BlockWorkerReceiveApi<WritableComparable> workerApi,
              Object executionStage) {
        return new InnerVertexReceiver() {
          @Override
          public void vertexReceive(Vertex<WritableComparable, LongWritable, Writable> vertex,
              Iterable<BooleanWritable> messages) {
            vertex.getValue().set(Iterables.size(messages));
          }
        };
      }

      @Override
      protected Class<BooleanWritable> getMessageClass() {
        return BooleanWritable.class;
      }
    }, new Object());

    Assert.assertEquals(1, graph.getVertex(new LongWritable(1)).getValue().get());
    Assert.assertEquals(2, graph.getVertex(new LongWritable(2)).getValue().get());
    Assert.assertEquals(1, graph.getVertex(new LongWritable(3)).getValue().get());
    Assert.assertEquals(0, graph.getVertex(new LongWritable(4)).getValue().get());
  }

  @Test
  public void testReducing() {
    TestGraph<LongWritable, LongWritable, NullWritable> graph = createTestGraph();

    final LongWritable value = new LongWritable();

    LocalBlockRunner.runBlock(graph, new Piece<WritableComparable, Writable, Writable, NoMessage, Object>() {
      private ReducerHandle<LongWritable, LongWritable> numVertices;

      @Override
      public void registerReducers(CreateReducersApi reduceApi, Object executionStage) {
        numVertices = reduceApi.createLocalReducer(SumReduce.LONG);
      }

      @Override
      public VertexSender<WritableComparable, Writable, Writable> getVertexSender(
          BlockWorkerSendApi<WritableComparable, Writable, Writable, NoMessage> workerApi,
          Object executionStage) {

        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<WritableComparable, Writable, Writable> vertex) {
            numVertices.reduce(new LongWritable(1));
          }
        };
      }

      @Override
      public void masterCompute(BlockMasterApi masterApi, Object executionStage) {
        value.set(numVertices.getReducedValue(masterApi).get());
      }
    }, new Object());

    Assert.assertEquals(4, value.get());
  }

  public void testVertexRemoval() {
    TestGraph<LongWritable, LongWritable, NullWritable> graph = createTestGraph();
    LocalBlockRunner.runBlock(graph, new Piece<LongWritable, Writable, Writable, NoMessage, Object>() {
      @Override
      public VertexSender<LongWritable, Writable, Writable> getVertexSender(
          final BlockWorkerSendApi<LongWritable, Writable, Writable, NoMessage> workerApi,
          Object executionStage) {
        return new InnerVertexSender() {
          @Override
          public void vertexSend(Vertex<LongWritable, Writable, Writable> vertex) {
            long id = vertex.getId().get();
            if (id == 1 || id == 3) {
              workerApi.removeVertexRequest(vertex.getId());
            }
          }
        };
      }
    }, new Object());

    Assert.assertNull(graph.getVertex(new LongWritable(1)));
    Assert.assertNotNull(graph.getVertex(new LongWritable(2)));
    Assert.assertNull(graph.getVertex(new LongWritable(3)));
    Assert.assertNotNull(graph.getVertex(new LongWritable(4)));
  }

  @Test
  public void testRepeatUntilBlockFinishCurrentLoop() throws Exception {
    final ObjectTransfer<Boolean> toQuit = new ObjectTransfer<>();
    final IntRef counter = new IntRef(5);
    Block counterPiece = Pieces.masterCompute("Count", new Consumer<BlockMasterApi>() {
      @Override
      public void apply(BlockMasterApi input) {
        counter.value--;
        if (counter.value == 0) {
          toQuit.apply(true);
        }
      }
    });
    Block innerBlock = new SequenceBlock(counterPiece, counterPiece, counterPiece, counterPiece);
    Block repeatBlock = RepeatUntilBlock.unlimited(
      innerBlock,
      toQuit
    );

    LocalBlockRunner.runBlock(createTestGraph(), repeatBlock, new Object());

    Assert.assertEquals(-3, counter.value);
  }

}
