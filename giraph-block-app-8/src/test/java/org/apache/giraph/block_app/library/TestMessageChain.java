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
package org.apache.giraph.block_app.library;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.combiner.MaxMessageCombiner;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.primitive.PrimitiveRefs.LongRef;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

/**
 * Tests and examples of using SendMessageChain
 */
public class TestMessageChain {

  private static GiraphConfiguration createConf() {
    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphConstants.VERTEX_ID_CLASS.set(conf, LongWritable.class);
    GiraphConstants.VERTEX_VALUE_CLASS.set(conf, LongWritable.class);
    GiraphConstants.EDGE_VALUE_CLASS.set(conf, NullWritable.class);
    return conf;
  }

  private static NumericTestGraph<LongWritable, LongWritable, NullWritable> createTestGraph() {
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph =
        new NumericTestGraph<LongWritable, LongWritable, NullWritable>(createConf());
    graph.addVertex(1);
    graph.addVertex(2);
    graph.addVertex(3);
    graph.addVertex(4);

    graph.addSymmetricEdge(1, 2);
    graph.addSymmetricEdge(2, 3);
    return graph;
  }

  @Test
  public void testReply() {
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph = createTestGraph();

    // calculates max ID of FOFs
    Block reply = SendMessageChain.<LongWritable, LongWritable, NullWritable, LongWritable>
    startSendToNeighbors(
        "SendMyIdToAllNeighbors",
        LongWritable.class,
        VertexSuppliers.vertexIdSupplier()
    ).thenSendToNeighbors(
        "SendMaxIReceivedToAllNeighbors",
        LongWritable.class,
        (vertex, messages) -> new LongWritable(max(messages))
    ).endConsume(
        (vertex, messages) -> vertex.getValue().set(max(messages))
    );

    LocalBlockRunner.runBlock(graph.getTestGraph(), reply, new Object());

    Assert.assertEquals(3, graph.getVertex(1).getValue().get());
    Assert.assertEquals(2, graph.getVertex(2).getValue().get());
    Assert.assertEquals(3, graph.getVertex(3).getValue().get());
    Assert.assertEquals(0, graph.getVertex(4).getValue().get());
  }

  @Test
  public void testReplyCombiner() {
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph = createTestGraph();

    // calculates max ID of FOFs
    Block reply = SendMessageChain.<LongWritable, LongWritable, NullWritable, LongWritable>
    startSendToNeighbors(
        "SendMyIdToAllNeighbors",
        MaxMessageCombiner.LONG,
        VertexSuppliers.vertexIdSupplier()
    ).thenSendToNeighbors(
        "SendMaxIReceivedToAllNeighbors",
        MaxMessageCombiner.LONG,
        (vertex, message) -> message
    ).endConsume(
        (vertex, message) -> vertex.getValue().set(message != null ? message.get() : 0)
    );

    LocalBlockRunner.runBlock(graph.getTestGraph(), reply, new Object());

    Assert.assertEquals(3, graph.getVertex(1).getValue().get());
    Assert.assertEquals(2, graph.getVertex(2).getValue().get());
    Assert.assertEquals(3, graph.getVertex(3).getValue().get());
    Assert.assertEquals(0, graph.getVertex(4).getValue().get());
  }

  @Test
  public void testReplyCombinerEndReduce() {
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph = createTestGraph();

    LongRef sumOfAll = new LongRef(0);

    // calculates max ID of FOFs
    Block reply = SendMessageChain.<LongWritable, LongWritable, NullWritable, LongWritable>
    startSendToNeighbors(
        "SendMyIdToAllNeighbors",
        MaxMessageCombiner.LONG,
        VertexSuppliers.vertexIdSupplier()
    ).thenSendToNeighbors(
        "SendMaxIReceivedToAllNeighbors",
        MaxMessageCombiner.LONG,
        (vertex, message) -> message
    ).endReduce(
        "SumAllReceivedValues",
        SumReduce.LONG,
        (vertex, message) -> message != null ? message : new LongWritable(0),
        (value) -> sumOfAll.value = value.get()
    );

    LocalBlockRunner.runBlock(
        graph.getTestGraph(),
        new SequenceBlock(
            reply,
            Pieces.forAllVertices(
                "SetAllValuesToReduced",
                (vertex) -> ((LongWritable) vertex.getValue()).set(sumOfAll.value))),
        new Object());

    Assert.assertEquals(8, graph.getVertex(1).getValue().get());
    Assert.assertEquals(8, graph.getVertex(2).getValue().get());
    Assert.assertEquals(8, graph.getVertex(3).getValue().get());
    Assert.assertEquals(8, graph.getVertex(4).getValue().get());

    // Block execution is happening in the separate environment if SERIALIZE_MASTER is used,
    // so our instance of sumOfAll will be unchanged
    Assert.assertEquals(LocalBlockRunner.SERIALIZE_MASTER.getDefaultValue() ? 0 : 8, sumOfAll.value);
  }


  @Test
  public void testStartCustom() {
    NumericTestGraph<LongWritable, LongWritable, NullWritable> graph = createTestGraph();

    Block reply = SendMessageChain.<LongWritable, LongWritable, NullWritable, LongWritable>
    startCustom(
        // Sends ID to it's first neighbor, passing max of received messages to next part of the chain
        (consumer) -> new Piece<LongWritable, LongWritable, NullWritable, LongWritable, Object>() {
          @Override
          public VertexSender<LongWritable, LongWritable, NullWritable> getVertexSender(
              BlockWorkerSendApi<LongWritable, LongWritable, NullWritable, LongWritable> workerApi,
              Object executionStage) {
            return (vertex) -> {
              Edge<LongWritable, NullWritable> edge =
                  Iterators.getNext(vertex.getEdges().iterator(), null);
              if (edge != null) {
                workerApi.sendMessage(edge.getTargetVertexId(), vertex.getId());
              }
            };
          }

          @Override
          public VertexReceiver<LongWritable, LongWritable, NullWritable, LongWritable>
              getVertexReceiver(BlockWorkerReceiveApi<LongWritable> workerApi, Object executionStage) {
            return (vertex, messages) -> {
              consumer.apply(vertex, new LongWritable(max(messages)));
            };
          }

          @Override
          protected Class<LongWritable> getMessageClass() {
            return LongWritable.class;
          }
        }
    ).thenSendToNeighbors(
        "SendMaxIReceivedToAllNeighbors",
        SumMessageCombiner.LONG,
        (vertex, message) -> message
    ).endConsume(
        (vertex, message) -> vertex.getValue().set(message != null ? message.get() : 0)
    );

    LocalBlockRunner.runBlock(graph.getTestGraph(), reply, new Object());

    Assert.assertEquals(3, graph.getVertex(1).getValue().get());
    Assert.assertEquals(2, graph.getVertex(2).getValue().get());
    Assert.assertEquals(3, graph.getVertex(3).getValue().get());
    Assert.assertEquals(0, graph.getVertex(4).getValue().get());
  }




  private static long max(Iterable<LongWritable> messages) {
    long result = 0;
    for (LongWritable message : messages) {
      result = Math.max(result, message.get());
    }
    return result;
  }
}
