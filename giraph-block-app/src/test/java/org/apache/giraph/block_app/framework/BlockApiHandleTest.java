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

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.BlockWithApiHandle;
import org.apache.giraph.block_app.framework.block.PieceCount;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.DefaultParentPiece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test the use of {@link BlockApiHandle}.
 */
public class BlockApiHandleTest {

  private static GiraphConfiguration createConf() {
    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphConstants.VERTEX_ID_CLASS.set(conf, LongWritable.class);
    GiraphConstants.VERTEX_VALUE_CLASS.set(conf, LongWritable.class);
    GiraphConstants.EDGE_VALUE_CLASS.set(conf, NullWritable.class);
    return conf;
  }

  private static TestGraph<LongWritable, LongWritable, NullWritable>
  createTestGraph() {
    TestGraph<LongWritable, LongWritable, NullWritable> graph =
      new TestGraph<>(createConf());
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

  public static class DummyObjectWithApiHandle {

    private BlockApiHandle handle;

    public DummyObjectWithApiHandle(BlockApiHandle handle) {
      this.handle = handle;
    }

    public void doSomethingAtWorker() {
      // checking that the handles have been set
      assertFalse(handle.isMasterApiSet());
      assertFalse(handle.isWorkerContextReceiveApiSet());
      assertFalse(handle.isWorkerContextSendApiSet());
      assertEquals(1, handle.getWorkerReceiveApi().getWorkerCount());
      assertEquals(0, handle.getWorkerSendApi().getMyWorkerIndex());
    }

    public void doSomethingAtWorkerContext() {
      // checking that the handles have been set
      assertFalse(handle.isMasterApiSet());
      assertFalse(handle.isWorkerReceiveApiSet());
      assertFalse(handle.isWorkerSendApiSet());
      assertEquals(1, handle.getWorkerContextReceiveApi().getWorkerCount());
      assertEquals(0, handle.getWorkerContextSendApi().getMyWorkerIndex());
    }

    public void doSomethingAtMaster() {
      // checking that the handles have been set
      assertEquals(1, handle.getMasterApi().getWorkerCount());
      assertFalse(handle.isWorkerReceiveApiSet());
      assertFalse(handle.isWorkerSendApiSet());
      assertFalse(handle.isWorkerContextReceiveApiSet());
      assertFalse(handle.isWorkerContextSendApiSet());
    }
  }

  public static class TestPiece extends DefaultParentPiece<WritableComparable,
      LongWritable, Writable, NullWritable, Object, DoubleWritable, Object> {

    private DummyObjectWithApiHandle object;

    public TestPiece(DummyObjectWithApiHandle object) {
      this.object = object;
    }

    @Override
    public VertexSender<WritableComparable, LongWritable, Writable>
    getVertexSender(final BlockWorkerSendApi<WritableComparable, LongWritable,
      Writable, NullWritable> workerApi, Object executionStage) {
      return new InnerVertexSender() {
        @Override
        public void vertexSend(
          Vertex<WritableComparable, LongWritable, Writable> vertex) {
          object.doSomethingAtWorker();
        }
      };
    }

    @Override
    public VertexReceiver<WritableComparable, LongWritable, Writable,
      NullWritable> getVertexReceiver(
      BlockWorkerReceiveApi<WritableComparable> workerApi,
      Object executionStage) {
      return new InnerVertexReceiver() {
        @Override
        public void vertexReceive(
          Vertex<WritableComparable, LongWritable, Writable> vertex,
          Iterable<NullWritable> messages) {
          object.doSomethingAtWorker();
        }
      };
    }

    public void workerContextSend(BlockWorkerContextSendApi<WritableComparable,
        DoubleWritable> workerContextApi, Object executionStage,
        Writable workerValue) {
      object.doSomethingAtWorkerContext();
    }

    /**
     * Override to have worker context receive computation.
     *
     * Called once per worker, before all vertices are going to be processed
     * with getVertexReceiver.
     */
    public void workerContextReceive(
      BlockWorkerContextReceiveApi workerContextApi, Object executionStage,
      Object workerValue, List<DoubleWritable> workerMessages) {
      object.doSomethingAtWorkerContext();
    }

    @Override
    public void masterCompute(BlockMasterApi masterApi, Object executionStage) {
      object.doSomethingAtMaster();
    }

    @Override
    protected Class<NullWritable> getMessageClass() {
      return NullWritable.class;
    }
  }

  @Test
  public void testBlockApiHandle() {
    TestGraph<LongWritable, LongWritable, NullWritable> graph =
      createTestGraph();

    final BlockApiHandle handle = new BlockApiHandle();
    final DefaultParentPiece piece =
      new TestPiece(new DummyObjectWithApiHandle(handle));

    Block block = new BlockWithApiHandle() {
      @Override
      public Iterator<AbstractPiece> iterator() {
        return piece.iterator();
      }

      @Override
      public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
        piece.forAllPossiblePieces(consumer);
      }

      @Override
      public PieceCount getPieceCount() {
        return piece.getPieceCount();
      }

      @Override
      public BlockApiHandle getBlockApiHandle() {
        return handle;
      }
    };

    BlockUtils.BLOCK_WORKER_CONTEXT_VALUE_CLASS.set(
      graph.getConf(), Object.class);
    LocalBlockRunner.runBlock(graph, block, new Object());
  }
}
