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
package org.apache.giraph.block_app.library.algo;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Example Application of BFS calculation
 */
public class BreadthFirstSearchBlockFactory extends AbstractBlockFactory<Object> {
  @Override
  public Block createBlock(GiraphConfiguration conf) {
    SupplierFromVertex<LongWritable, BreadthFirstSearchVertexValue, Writable, Boolean>
    isVertexInSeedSet =
      (vertex) -> {
        return vertex.getValue().isSeedVertex();
      };

    SupplierFromVertex<LongWritable, BreadthFirstSearchVertexValue, Writable, IntWritable>
    getDistance =
      (vertex) -> {
        return new IntWritable(vertex.getValue().getDistance());
      };

    ConsumerWithVertex<LongWritable, BreadthFirstSearchVertexValue, Writable, IntWritable>
    setDistance =
      (vertex, value) -> {
        vertex.getValue().setDistance(value.get());
      };

    return BreadthFirstSearch.bfs(
      isVertexInSeedSet,
      getDistance,
      setDistance);
  }

  @Override
  public Object createExecutionStage(GiraphConfiguration conf) {
    return new Object();
  }

  @Override
  protected Class<LongWritable> getVertexIDClass(GiraphConfiguration conf) {
    return LongWritable.class;
  }

  @Override
  protected Class<BreadthFirstSearchVertexValue> getVertexValueClass(GiraphConfiguration conf) {
    return BreadthFirstSearchVertexValue.class;
  }

  @Override
  protected Class<NullWritable> getEdgeValueClass(GiraphConfiguration conf) {
    return NullWritable.class;
  }
}
