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


import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphChecker;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.conf.BulkConfigurator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

public class SendingMessagesTest {
  @Test
  public void createVertexOnMsgsTest() throws Exception {
    TestGraphUtils.runTest(
        new TestGraphModifier<LongWritable, LongWritable, Writable>() {
          @Override
          public void modifyGraph(NumericTestGraph<LongWritable, LongWritable, Writable> graph) {
            graph.addEdge(1, 2);
          }
        },
        new TestGraphChecker<LongWritable, LongWritable, Writable>() {
          @Override
          public void checkOutput(NumericTestGraph<LongWritable, LongWritable, Writable> graph) {
            Assert.assertEquals(1, graph.getValue(2).get());
            Assert.assertEquals(0, graph.getValue(1).get());
          }
        },
        new BulkConfigurator() {
          @Override
          public void configure(GiraphConfiguration conf) {
            BlockUtils.setBlockFactoryClass(conf, SendingMessagesToNeighborsBlockFactory.class);
          }
        });
  }

  @Test
  public void doNotCreateVertexOnMsgsTest() throws Exception {
    TestGraphUtils.runTest(
        new TestGraphModifier<LongWritable, LongWritable, Writable>() {
          @Override
          public void modifyGraph(NumericTestGraph<LongWritable, LongWritable, Writable> graph) {
            graph.addEdge(1, 2);
          }
        },
        new TestGraphChecker<LongWritable, LongWritable, Writable>() {
          @Override
          public void checkOutput(NumericTestGraph<LongWritable, LongWritable, Writable> graph) {
            Assert.assertNull(graph.getVertex(2));
            Assert.assertEquals(0, graph.getValue(1).get());
          }
        },
        new BulkConfigurator() {
          @Override
          public void configure(GiraphConfiguration conf) {
            BlockUtils.setBlockFactoryClass(conf, SendingMessagesToNeighborsBlockFactory.class);
            GiraphConstants.RESOLVER_CREATE_VERTEX_ON_MSGS.set(conf, false);
          }
        });
  }

  @Test
  public void createMultiMsgs() throws Exception {
    TestGraphUtils.runTest(
        new TestGraphModifier<LongWritable, LongWritable, Writable>() {
          @Override
          public void modifyGraph(NumericTestGraph<LongWritable, LongWritable, Writable> graph) {
            graph.addSymmetricEdge(1, 2);
            graph.addSymmetricEdge(3, 2);
          }
        },
        new TestGraphChecker<LongWritable, LongWritable, Writable>() {
          @Override
          public void checkOutput(NumericTestGraph<LongWritable, LongWritable, Writable> graph) {
            Assert.assertEquals(3, graph.getValue(2).get());
            Assert.assertEquals(2, graph.getValue(1).get());
            Assert.assertEquals(2, graph.getValue(3).get());
          }
        },
        new BulkConfigurator() {
          @Override
          public void configure(GiraphConfiguration conf) {
            BlockUtils.setBlockFactoryClass(conf, SendingMessagesToNeighborsBlockFactory.class);
          }
        });
  }

  public static class SendingMessagesToNeighborsBlockFactory extends TestLongNullNullBlockFactory {
    @Override
    protected Class<? extends Writable> getVertexValueClass(GiraphConfiguration conf) {
      return LongWritable.class;
    }

    @Override
    public Block createBlock(GiraphConfiguration conf) {
      return Pieces.sendMessageToNeighbors(
          "SendToNeighbors",
          LongWritable.class,
          VertexSuppliers.<LongWritable, LongWritable, Writable>vertexIdSupplier(),
          new ConsumerWithVertex<LongWritable, LongWritable, Writable, Iterable<LongWritable>>() {
            @Override
            public void apply(Vertex<LongWritable, LongWritable, Writable> vertex,
                Iterable<LongWritable> messages) {
              long max = 0;
              for (LongWritable v : messages) {
                max = Math.max(max, v.get());
              }
              vertex.getValue().set(max);
            }
          });
    }
  }
}
