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

package org.apache.giraph.block_app.library.triangles;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.IfBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.prepare_graph.PrepareGraphPieces;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.LongNullHashSetEdges;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.longs.LongListIterator;

/**
 * Count triangles in undirected graph.
 * Assumes that edges in both directions are present, or optionally makes
 * graph symmetric first.
 */
public class UndirectedTriangleCountingBlockFactory
    extends AbstractBlockFactory<Object> {
  /**
   * Set to true to make graph symmetric before calculation if it's not already
   */
  public static final BooleanConfOption MAKE_GRAPH_SYMMETRIC =
      new BooleanConfOption("giraph.triangleCounting.makeGraphSymmetric",
          false, "Whether it's needed to first make graph symmetric or not");

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
        new IfBlock(() -> MAKE_GRAPH_SYMMETRIC.get(conf),
            PrepareGraphPieces.makeSymmetricUnweighted(LongTypeOps.INSTANCE)),
        new UndirectedCountTrianglesPiece());
  }

  @Override
  public Object createExecutionStage(GiraphConfiguration conf) {
    return new Object();
  }

  @Override
  protected void additionalInitConfig(GiraphConfiguration conf) {
    conf.setOutEdgesClass(LongNullHashSetEdges.class);
    GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE.setIfUnset(
        conf, MessageEncodeAndStoreType.POINTER_LIST_PER_VERTEX);
  }

  /**
   * Piece in which all vertices will send to their neighbors the list of their
   * neighbors and vertices will count how many of received ids are their
   * neighbors.
   * TODO could be striped to make memory requirements lower
   */
  public static class UndirectedCountTrianglesPiece extends
      Piece<LongWritable, LongWritable, NullWritable, WLongArrayList, Object> {
    @Override
    public VertexSender<LongWritable, LongWritable, NullWritable>
    getVertexSender(
        BlockWorkerSendApi<LongWritable, LongWritable, NullWritable,
            WLongArrayList> workerApi,
        Object executionStage) {
      WLongArrayList reusableMessage = new WLongArrayList();
      return vertex -> {
        reusableMessage.clear();
        for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
          // TODO We could only send edge once
          reusableMessage.add(edge.getTargetVertexId().get());
        }
        workerApi.sendMessageToAllEdges(vertex, reusableMessage);
      };
    }

    @Override
    public VertexReceiver<LongWritable, LongWritable, NullWritable,
        WLongArrayList> getVertexReceiver(
        BlockWorkerReceiveApi<LongWritable> workerApi, Object executionStage) {
      LongWritable reusableId = new LongWritable();
      return (vertex, messages) -> {
        long numTriangles = 0;
        for (WLongArrayList message : messages) {
          LongListIterator iter = message.iterator();
          while (iter.hasNext()) {
            reusableId.set(iter.nextLong());
            if (reusableId.get() != vertex.getId().get() &&
                vertex.getEdgeValue(reusableId) != null) {
              numTriangles++;
            }
          }
        }
        Preconditions.checkArgument(numTriangles % 2 == 0);
        vertex.getValue().set(numTriangles / 2);
      };
    }

    @Override
    protected Class<WLongArrayList> getMessageClass() {
      return WLongArrayList.class;
    }

    @Override
    protected boolean allowOneMessageToManyIdsEncoding() {
      return true;
    }
  }
}
