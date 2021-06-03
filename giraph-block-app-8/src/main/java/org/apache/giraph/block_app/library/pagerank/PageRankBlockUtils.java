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

package org.apache.giraph.block_app.library.pagerank;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Utility class that helps to construct page rank block
 */
public class PageRankBlockUtils {
  /** Do not instantiate */
  private PageRankBlockUtils() {
  }

  public static <I extends WritableComparable, V extends Writable> Block
  weightedPagerank(
      ConsumerWithVertex<I, V, DoubleWritable, DoubleWritable> valueSetter,
      SupplierFromVertex<I, V, DoubleWritable, DoubleWritable> valueGetter,
      GiraphConfiguration conf) {
    return new SequenceBlock(
        new PageRankInitializeAndNormalizeEdgesPiece<>(valueSetter, conf),
        pagerank(valueSetter, valueGetter,
            (vertex, edgeValue) -> edgeValue.get(), conf));
  }

  public static <I extends WritableComparable, V extends Writable> Block
  unweightedPagerank(
      ConsumerWithVertex<I, V, NullWritable, DoubleWritable> valueSetter,
      SupplierFromVertex<I, V, NullWritable, DoubleWritable> valueGetter,
      GiraphConfiguration conf) {
    return pagerank(valueSetter, valueGetter,
        (UnweightedEdgeValueGetter<I, V, NullWritable>)
            vertex ->
                vertex.getNumEdges() == 0 ? 0 : 1.0 / vertex.getNumEdges(),
        conf);
  }

  public static <I extends WritableComparable, V extends Writable,
      E extends Writable> Block pagerank(
      ConsumerWithVertex<I, V, E, DoubleWritable> valueSetter,
      SupplierFromVertex<I, V, E, DoubleWritable> valueGetter,
      EdgeValueGetter<I, V, E> edgeValueGetter,
      GiraphConfiguration conf) {
    ObjectTransfer<Boolean> haltCondition = new ObjectTransfer<>();
    ObjectTransfer<DoubleWritable> valueTransfer = new ObjectTransfer<>();
    return new SequenceBlock(
        new RepeatUntilBlock(PageRankSettings.getIterations(conf) + 1,
            new PageRankIteration<>(valueSetter, valueGetter, edgeValueGetter,
                valueTransfer, haltCondition, conf),
            haltCondition
        )
    );
  }
}
