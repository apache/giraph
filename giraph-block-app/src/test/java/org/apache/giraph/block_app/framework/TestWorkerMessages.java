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

import java.util.HashSet;
import java.util.List;

import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.PieceWithWorkerContext;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.conf.BulkConfigurator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test sending worker to worker messages
 */
public class TestWorkerMessages {
  @Test
  public void testWorkerMessages() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    BlockUtils.setAndInitBlockFactoryClass(conf, TestWorkerMessagesBlockFactory.class);
    TestGraph testGraph = new TestGraph(conf);
    testGraph.addEdge(new LongWritable(1), new LongWritable(2), NullWritable.get());
    LocalBlockRunner.runApp(testGraph);
  }

  @Test
  public void testWithTestSetup() throws Exception {
    TestGraphUtils.runTest(
        new TestGraphModifier<WritableComparable, Writable, Writable>() {
          @Override
          public void modifyGraph(NumericTestGraph<WritableComparable, Writable, Writable> graph) {
            graph.addEdge(1, 2);
          }
        },
        null,
        new BulkConfigurator() {
          @Override
          public void configure(GiraphConfiguration conf) {
            BlockUtils.setBlockFactoryClass(conf, TestWorkerMessagesBlockFactory.class);
          }
        });
  }

  public static class TestWorkerMessagesBlockFactory extends TestLongNullNullBlockFactory {
    @Override
    public Block createBlock(GiraphConfiguration conf) {
      return new SequenceBlock(
          new TestWorkerMessagesPiece(2, 4, 11),
          new TestWorkerMessagesPiece(3, 5, 2, 100));
    }
  }

  public static class TestWorkerMessagesPiece extends PieceWithWorkerContext<LongWritable,
      Writable, Writable, NoMessage, Object, LongWritable, Object> {
    private final HashSet<Long> values;

    public TestWorkerMessagesPiece(long... values) {
      this.values = new HashSet<>();
      for (long value : values) {
        this.values.add(value);
      }
    }

    @Override
    public void workerContextSend(
        BlockWorkerContextSendApi<LongWritable, LongWritable> workerContextApi,
        Object executionStage, Object workerValue) {
      for (long value : values) {
        workerContextApi.sendMessageToWorker(new LongWritable(value),
            workerContextApi.getMyWorkerIndex());
      }
    }

    @Override
    public void workerContextReceive(BlockWorkerContextReceiveApi workerContextApi,
        Object executionStage, Object workerValue, List<LongWritable> workerMessages) {
      Assert.assertEquals(values.size(), workerMessages.size());
      for (LongWritable workerMessage : workerMessages) {
        Assert.assertTrue(values.remove(workerMessage.get()));
      }
    }
  }
}
