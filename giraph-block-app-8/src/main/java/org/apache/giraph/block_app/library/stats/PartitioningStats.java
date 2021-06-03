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
package org.apache.giraph.block_app.library.stats;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.library.SendMessageChain;
import org.apache.giraph.function.primitive.DoubleConsumer;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.reducers.impl.PairReduce;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Utility blocks for calculating stats for a given partitioning - an
 * assignment of vertices to buckets.
 */
public class PartitioningStats {
  private static final Logger LOG = Logger.getLogger(PartitioningStats.class);

  private PartitioningStats() { }

  /**
   * Calculate edge locality - ratio of edges that are within a same bucket.
   */
  public static <V extends Writable> Block calculateEdgeLocality(
      SupplierFromVertex<WritableComparable, V, Writable, LongWritable>
        bucketSupplier,
      DoubleConsumer edgeLocalityConsumer) {
    final Pair<LongWritable, LongWritable> pair =
        Pair.of(new LongWritable(), new LongWritable());
    return SendMessageChain.<WritableComparable, V, Writable, LongWritable>
    startSendToNeighbors(
        "CalcLocalEdgesPiece",
        LongWritable.class,
        bucketSupplier
    ).endReduceWithMaster(
        "AggregateEdgeLocalityPiece",
        new PairReduce<>(SumReduce.LONG, SumReduce.LONG),
        (vertex, messages) -> {
          long bucket = bucketSupplier.get(vertex).get();
          int local = 0;
          int total = 0;
          for (LongWritable otherCluster : messages) {
            total++;
            if (bucket == otherCluster.get()) {
              local++;
            }
          }
          pair.getLeft().set(local);
          pair.getRight().set(total);
          return pair;
        },
        (reducedPair, master) -> {
          long localEdges = reducedPair.getLeft().get();
          long totalEdges = reducedPair.getRight().get();
          double edgeLocality = (double) localEdges / totalEdges;
          LOG.info("locality ratio = " + edgeLocality);
          master.getCounter(
              "Edge locality stats", "edge locality (in percent * 1000)")
            .setValue((long) (edgeLocality * 100000));
          edgeLocalityConsumer.apply(edgeLocality);
        }
    );
  }

  /**
   * Calculates average fanout - average number of distinct buckets that vertex
   * has neighbors in.
   */
  public static <V extends Writable> Block calculateFanout(
      SupplierFromVertex<WritableComparable, V, Writable, LongWritable>
        bucketSupplier,
      DoubleConsumer averageFanoutConsumer) {
    final Pair<LongWritable, LongWritable> pair =
        Pair.of(new LongWritable(), new LongWritable(1));
    return SendMessageChain.<WritableComparable, V, Writable, LongWritable>
    startSendToNeighbors(
        "CalcFanoutPiece",
        LongWritable.class,
        bucketSupplier
    ).endReduceWithMaster(
        "AggregateFanoutPiece",
        new PairReduce<>(SumReduce.LONG, SumReduce.LONG),
        (vertex, messages) -> {
          LongSet setOfNeighborBuckets = new LongOpenHashSet();
          for (LongWritable neighborBucket : messages) {
            setOfNeighborBuckets.add(neighborBucket.get());
          }
          pair.getLeft().set(setOfNeighborBuckets.size());
          return pair;
        },
        (reducedPair, master) -> {
          long fanout = reducedPair.getLeft().get();
          long numVertices = reducedPair.getRight().get();
          double avgFanout = (double) fanout / numVertices;
          LOG.info("fanout ratio = " + avgFanout);
          master.getCounter("Fanout stats", "fanout (in percent * 1000)")
            .setValue((long) (avgFanout * 100000));
          averageFanoutConsumer.apply(avgFanout);
        }
    );
  }
}
