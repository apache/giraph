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
package org.apache.giraph.examples.darwini;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.EmptyBlock;
import org.apache.giraph.block_app.framework.block.RepeatBlock;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Merge multiple super-communities (countries) into single graph.
 * The idea is that we don't need to keep all edges to merge smaller
 * graphs into bigger one. We only need to keep meta-information like
 * number of edges. That saves a lot of memory and allows us to create
 * graphs we are incapable of handling now otherwise.
 */
public class MergeGraphFactory extends AbstractBlockFactory<Integer> {

  /**
   * Mimimum size of the community.
   */
  public static final int STARTING_STAGE = 3;


  @Override
  protected Class<? extends WritableComparable> getVertexIDClass(
      GiraphConfiguration conf) {
    return LongWritable.class;
  }

  @Override
  protected Class<? extends Writable> getVertexValueClass(
      GiraphConfiguration conf) {
    return VertexData.class;
  }

  @Override
  protected Class<? extends Writable> getEdgeValueClass(
      GiraphConfiguration conf) {
    return NullWritable.class;
  }


  @Override
  public Block createBlock(GiraphConfiguration conf) {
    ObjectTransfer<LongWritable> targetTransfer = new ObjectTransfer<>();
    ObjectTransfer<Boolean> randomEdgesHalt = new ObjectTransfer<>();

    return new SequenceBlock(
        new RepeatBlock(Constants.MAX_GEO_SPAN.get(conf) - STARTING_STAGE,
            new SequenceBlock(
                createGeoRandomEdges(conf),
                new NextStage(Integer.MAX_VALUE)
            )),

        new RepeatUntilBlock(Constants.RANDOM_EDGE_REQUEST_SUPERSTEPS.get(conf),
            new SequenceBlock(
                new MergeRequestRandomEdges(targetTransfer),
                new MergeCreateRandomEdges(targetTransfer, randomEdgesHalt)),
            randomEdgesHalt)
    );
  }

  /**
   * Creates a block that makes random edges using communities.
   * @param conf job configuration
   * @return block that makes random edges using communities.
   */
  private Block createGeoRandomEdges(GiraphConfiguration conf) {
    if (Constants.USE_RANDOM_GEO.get(conf) > 0) {
      ObjectTransfer<Iterable<RandomEdgeRequest>> geographyMessages =
          new ObjectTransfer<>();
      return new RepeatBlock(Constants.USE_RANDOM_GEO.get(conf),
          new SequenceBlock(
              new MergeRequestGeoRandomEdges(geographyMessages),
              new MergeCreateGeoRandomEdges(geographyMessages))
      );
    }
    return new EmptyBlock();
  }

  @Override
  public Integer createExecutionStage(GiraphConfiguration conf) {
    return STARTING_STAGE;
  }
}
