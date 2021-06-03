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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Example Application of distributed independent set calculation
 */
public class DistributedIndependentSetBlockFactory extends AbstractBlockFactory<Object> {
  @Override
  public Block createBlock(GiraphConfiguration conf) {
    return DistributedIndependentSet.
        <LongWritable, DistributedIndependentSetVertexValue>independentSets(
            getVertexIDClass(conf),
            (vertex) -> vertex.getValue().getIndependentSetID(),
            (vertex, value) -> vertex.getValue().setIndependentSetID(value)
        );
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
  protected Class<DistributedIndependentSetVertexValue> getVertexValueClass(
      GiraphConfiguration conf) {
    return DistributedIndependentSetVertexValue.class;
  }

  @Override
  protected Class<NullWritable> getEdgeValueClass(GiraphConfiguration conf) {
    return NullWritable.class;
  }
}
