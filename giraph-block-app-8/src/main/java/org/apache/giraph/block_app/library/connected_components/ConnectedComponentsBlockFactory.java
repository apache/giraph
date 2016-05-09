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

package org.apache.giraph.block_app.library.connected_components;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.IfBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.prepare_graph.PrepareGraphPieces;
import org.apache.giraph.block_app.library.prepare_graph.UndirectedConnectedComponents;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.LongNullArrayEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Block factory for connected components
 */
public class ConnectedComponentsBlockFactory
    extends AbstractBlockFactory<Object> {
  /**
   * If input graph is already symmetric this can be set to false to skip
   * symmetrizing step, otherwise keep it true in order to get correct results.
   */
  public static final BooleanConfOption MAKE_GRAPH_SYMMETRIC =
      new BooleanConfOption("giraph.connectedComponents.makeGraphSymmetric",
          true, "Whether it's needed to first make graph symmetric or not");

  @Override
  protected Class<? extends WritableComparable> getVertexIDClass(
      GiraphConfiguration conf) {
    return LongWritable.class;
  }

  @Override
  protected Class<? extends Writable> getVertexValueClass(
      GiraphConfiguration conf) {
    return LongWritable.class;
  }

  @Override
  protected Class<? extends Writable> getEdgeValueClass(
      GiraphConfiguration conf) {
    return NullWritable.class;
  }

  @Override
  public Block createBlock(GiraphConfiguration conf) {
    return new SequenceBlock(
        // Make graph symmetric if needed
        new IfBlock(() -> MAKE_GRAPH_SYMMETRIC.get(conf),
            PrepareGraphPieces.makeSymmetricUnweighted(LongTypeOps.INSTANCE)),
        UndirectedConnectedComponents.
            <LongWritable>calculateConnectedComponents(
                Integer.MAX_VALUE,
                Vertex::getValue,
                (vertex, component) -> vertex.getValue().set(component.get())));
  }

  @Override
  public Object createExecutionStage(GiraphConfiguration conf) {
    return new Object();
  }

  @Override
  protected void additionalInitConfig(GiraphConfiguration conf) {
    // Save on network traffic by only sending one message value per worker
    GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE.setIfUnset(
        conf, MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION);
    conf.setOutEdgesClass(LongNullArrayEdges.class);
  }
}
