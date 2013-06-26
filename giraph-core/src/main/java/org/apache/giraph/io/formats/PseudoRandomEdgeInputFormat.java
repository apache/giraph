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

package org.apache.giraph.io.formats;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * This {@link EdgeInputFormat} generates pseudo-random edges on the fly.
 * As with {@link PseudoRandomVertexInputFormat}, the user specifies the
 * number of vertices and the number of edges per vertex.
 */
public class PseudoRandomEdgeInputFormat
    extends EdgeInputFormat<LongWritable, DoubleWritable> {
  @Override public void checkInputSpecs(Configuration conf) { }

  @Override
  public final List<InputSplit> getSplits(final JobContext context,
                                          final int minSplitCountHint)
    throws IOException, InterruptedException {
    return PseudoRandomUtils.getSplits(minSplitCountHint);
  }

  @Override
  public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new PseudoRandomEdgeReader();
  }

  /**
   * {@link EdgeReader} that generates pseudo-random edges.
   */
  private static class PseudoRandomEdgeReader
      extends EdgeReader<LongWritable, DoubleWritable>  {
    /** Logger. */
    private static final Logger LOG =
        Logger.getLogger(PseudoRandomEdgeReader.class);
    /** Starting vertex id. */
    private long startingVertexId = -1;
    /** Vertices read so far. */
    private long verticesRead = 0;
    /** Total vertices to read (on this split alone). */
    private long totalSplitVertices = -1;
    /** Current vertex id. */
    private LongWritable currentVertexId = new LongWritable(-1);
    /** Edges read for the current vertex. */
    private int currentOutEdgesRead = 0;
    /** Target vertices of edges for current vertex. */
    private Set<LongWritable> currentVertexDestVertices = Sets.newHashSet();
    /** Random number generator for the current vertex (for consistency
     * across runs on different numbers of workers). */
    private Random random = new Random();
    /** Aggregate vertices (all input splits). */
    private long aggregateVertices = -1;
    /** Edges per vertex. */
    private int edgesPerVertex = -1;
    /** BspInputSplit (used only for index). */
    private BspInputSplit bspInputSplit;
    /** Helper for generating pseudo-random local edges. */
    private PseudoRandomLocalEdgesHelper localEdgesHelper;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
      aggregateVertices = getConf().getLong(
              PseudoRandomInputFormatConstants.AGGREGATE_VERTICES, 0);
      if (aggregateVertices <= 0) {
        throw new IllegalArgumentException(
            PseudoRandomInputFormatConstants.AGGREGATE_VERTICES + " <= 0");
      }
      if (inputSplit instanceof BspInputSplit) {
        bspInputSplit = (BspInputSplit) inputSplit;
        long extraVertices =
            aggregateVertices % bspInputSplit.getNumSplits();
        totalSplitVertices =
            aggregateVertices / bspInputSplit.getNumSplits();
        if (bspInputSplit.getSplitIndex() < extraVertices) {
          ++totalSplitVertices;
        }
        startingVertexId = (bspInputSplit.getSplitIndex() *
            (aggregateVertices / bspInputSplit.getNumSplits())) +
            Math.min(bspInputSplit.getSplitIndex(),
                extraVertices);
      } else {
        throw new IllegalArgumentException(
            "initialize: Got " + inputSplit.getClass() +
                " instead of " + BspInputSplit.class);
      }
      edgesPerVertex = getConf().getInt(
          PseudoRandomInputFormatConstants.EDGES_PER_VERTEX, 0);
      if (edgesPerVertex <= 0) {
        throw new IllegalArgumentException(
            PseudoRandomInputFormatConstants.EDGES_PER_VERTEX + " <= 0");
      }
      float minLocalEdgesRatio = getConf().getFloat(
          PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO,
          PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO_DEFAULT);
      localEdgesHelper = new PseudoRandomLocalEdgesHelper(aggregateVertices,
          minLocalEdgesRatio, getConf());
    }

    @Override
    public boolean nextEdge() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead + 1 ||
          (totalSplitVertices == verticesRead + 1 &&
              edgesPerVertex > currentOutEdgesRead);
    }

    @Override
    public LongWritable getCurrentSourceId() throws IOException,
        InterruptedException {
      if (currentOutEdgesRead == edgesPerVertex) {
        ++verticesRead;
        currentVertexId = new LongWritable(-1);
      }

      if (currentVertexId.get() == -1) {
        currentVertexId.set(startingVertexId + verticesRead);
        currentOutEdgesRead = 0;
        // Seed on the vertex id to keep the vertex data the same when
        // on different number of workers, but other parameters are the
        // same.
        random.setSeed(currentVertexId.get());
        currentVertexDestVertices.clear();
      }
      return currentVertexId;
    }

    @Override
    public Edge<LongWritable, DoubleWritable> getCurrentEdge()
      throws IOException, InterruptedException {
      LongWritable destVertexId = new LongWritable();
      do {
        destVertexId.set(localEdgesHelper.generateDestVertex(
            currentVertexId.get(), random));
      } while (currentVertexDestVertices.contains(destVertexId));
      ++currentOutEdgesRead;
      currentVertexDestVertices.add(destVertexId);
      if (LOG.isTraceEnabled()) {
        LOG.trace("getCurrentEdge: Return edge (" + currentVertexId + ", " +
            "" + destVertexId + ")");
      }
      return EdgeFactory.create(
          destVertexId,
          new DoubleWritable(random.nextDouble()));
    }

    @Override
    public void close() throws IOException { }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return (verticesRead * edgesPerVertex + currentOutEdgesRead) *
          100.0f / (totalSplitVertices * edgesPerVertex);
    }
  }
}
