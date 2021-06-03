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

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.test_setup.NumericTestGraph;
import org.apache.giraph.block_app.test_setup.TestGraphChecker;
import org.apache.giraph.block_app.test_setup.TestGraphModifier;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.conf.BulkConfigurator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test when vertex gets multiple simultaneous mutations
 * (i.e. to non-existent vertex, send a message and do add edge request)
 * and confirm all mutations are correctly processed
 */
public class MultipleSimultanousMutationsTest {
  @Test
  public void createVertexOnMsgsTest() throws Exception {
    TestGraphUtils.runTest(
        new TestGraphModifier<LongWritable, Writable, LongWritable>() {
          @Override
          public void modifyGraph(NumericTestGraph<LongWritable, Writable, LongWritable> graph) {
            graph.addEdge(1, 2, 2);
          }
        },
        new TestGraphChecker<LongWritable, Writable, LongWritable>() {
          @Override
          public void checkOutput(NumericTestGraph<LongWritable, Writable, LongWritable> graph) {
            Assert.assertEquals(1, graph.getVertex(1).getNumEdges());
            Assert.assertNull(graph.getVertex(1).getEdgeValue(new LongWritable(-1)));
            Assert.assertEquals(2, graph.getVertex(1).getEdgeValue(new LongWritable(2)).get());

            Assert.assertEquals(1, graph.getVertex(2).getNumEdges());
            Assert.assertEquals(-1, graph.getVertex(2).getEdgeValue(new LongWritable(-1)).get());
          }
        },
        new BulkConfigurator() {
          @Override
          public void configure(GiraphConfiguration conf) {
            BlockUtils.setBlockFactoryClass(conf, SendingAndAddEdgeBlockFactory.class);
          }
        });
  }

  public static class SendingAndAddEdgeBlockFactory extends TestLongNullNullBlockFactory {
    @Override
    protected Class<? extends Writable> getEdgeValueClass(GiraphConfiguration conf) {
      return LongWritable.class;
    }

    @Override
    public Block createBlock(GiraphConfiguration conf) {
      return new Piece<LongWritable, Writable, LongWritable, NullWritable, Object>() {
        @Override
        protected Class<NullWritable> getMessageClass() {
          return NullWritable.class;
        }

        @Override
        public VertexSender<LongWritable, Writable, LongWritable> getVertexSender(
            final BlockWorkerSendApi<LongWritable, Writable, LongWritable, NullWritable> workerApi,
            Object executionStage) {
          final ReusableEdge<LongWritable, LongWritable> reusableEdge = workerApi.getConf().createReusableEdge();
          reusableEdge.setTargetVertexId(new LongWritable(-1));
          reusableEdge.setValue(new LongWritable(-1));
          return new VertexSender<LongWritable, Writable, LongWritable>() {
            @Override
            public void vertexSend(Vertex<LongWritable, Writable, LongWritable> vertex) {
              for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
                workerApi.addEdgeRequest(edge.getTargetVertexId(), reusableEdge);
                workerApi.sendMessage(edge.getTargetVertexId(), NullWritable.get());
              }
            }
          };
        }
      };
    }
  }
}
