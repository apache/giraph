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
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Entry point into darwini graph generation.
 */
public class GraphGenerationFactory extends AbstractBlockFactory<Integer> {


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
        createCommunities(),

        new RepeatUntilBlock(Constants.MAX_ITERATIONS.get(conf),
          new SequenceBlock(
              new RepeatBlock(Constants.RANDOM_ITERATIONS.get(conf),
                  new SequenceBlock(
                      new RequestRandomEdges(targetTransfer),
                      new CreateRandomEdges(targetTransfer, randomEdgesHalt)
                  )
              ),
              createGeoRandomEdges(conf),
              new NextStage(Constants.MAX_GEO_SPAN.get(conf))
          ), randomEdgesHalt));
  }

  /**
   * Block that creates communities.
   * @return block that creates communities
   */
  private Block createCommunities() {
    ObjectTransfer<Iterable<VertexInfo>> communityMessages =
        new ObjectTransfer<>();
    return new SequenceBlock(new BuildCommunities(),
        new CollectCommunityInfo(communityMessages),
        new CreateCommunityEdges(communityMessages));
  }

  /**
   * Block that creates random edges
   * @param conf job configuration
   * @return depending on configuration returns empty block
   * or block that creates random edges
   */
  private Block createGeoRandomEdges(GiraphConfiguration conf) {
    if (Constants.USE_RANDOM_GEO.get(conf) > 0) {
      ObjectTransfer<Iterable<RandomEdgeRequest>> geographyMessages =
          new ObjectTransfer<>();
      return new SequenceBlock(
              new RequestGeoRandomEdges(geographyMessages),
              new CreateGeoRandomEdges(geographyMessages));
    }
    return new EmptyBlock();
  }



  @Override
  public Integer createExecutionStage(GiraphConfiguration conf) {
    return 2;
  }

  @Override
  protected void additionalInitConfig(GiraphConfiguration conf) {
    GiraphConstants.NUM_INPUT_THREADS.set(conf, 1);
    GiraphConstants.NUM_OUTPUT_THREADS.set(conf, 20);
    GiraphConstants.VERTEX_OUTPUT_FORMAT_THREAD_SAFE.set(conf, true);
    conf.setOutEdgesClass(HashMapEdges.class);
  }

}
