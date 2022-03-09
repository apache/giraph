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

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * VertexInputFormat for large scale testing,
 * like {@link PseudoRandomVertexInputFormat}, but for the unweighted graphs
 * where vertex ids are integers.
 */
public class PseudoRandomIntNullVertexInputFormat extends
    VertexInputFormat<IntWritable, FloatWritable, NullWritable> {
  @Override public void checkInputSpecs(Configuration conf) { }

  @Override
  public final List<InputSplit> getSplits(final JobContext context,
      final int minSplitCountHint) throws IOException, InterruptedException {
    return PseudoRandomUtils.getSplits(minSplitCountHint);
  }

  @Override
  public VertexReader<IntWritable, FloatWritable, NullWritable>
  createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new PseudoRandomVertexReader();
  }

  /**
   * Used by {@link PseudoRandomIntNullVertexInputFormat} to read
   * pseudo-randomly generated data.
   */
  private static class PseudoRandomVertexReader extends
      VertexReader<IntWritable, FloatWritable, NullWritable> {
    /** Starting vertex id. */
    private int startingVertexId = -1;
    /** Vertices read so far. */
    private int verticesRead = 0;
    /** Total vertices to read (on this split alone). */
    private int totalSplitVertices = -1;
    /** Edges per vertex. */
    private int edgesPerVertex = -1;
    /** Reusable int set */
    private final IntSet destVertices = new IntOpenHashSet();
    /** Resuable edge object */
    private ReusableEdge<IntWritable, NullWritable> reusableEdge = null;
    /** Helper for generating pseudo-random local edges. */
    private PseudoRandomIntNullLocalEdgesHelper localEdgesHelper;
    /** Random */
    private Random rand;

    /** Default constructor for reflection. */
    public PseudoRandomVertexReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit,
        TaskAttemptContext context) throws IOException {
      int aggregateVertices = getConf().getInt(
          PseudoRandomInputFormatConstants.AGGREGATE_VERTICES, 0);
      BspInputSplit bspInputSplit = (BspInputSplit) inputSplit;
      int extraVertices = aggregateVertices % bspInputSplit.getNumSplits();
      totalSplitVertices = aggregateVertices / bspInputSplit.getNumSplits();
      if (bspInputSplit.getSplitIndex() < extraVertices) {
        ++totalSplitVertices;
      }
      startingVertexId = bspInputSplit.getSplitIndex() *
          (aggregateVertices / bspInputSplit.getNumSplits()) +
          Math.min(bspInputSplit.getSplitIndex(), extraVertices);
      edgesPerVertex = getConf().getInt(
          PseudoRandomInputFormatConstants.EDGES_PER_VERTEX, 0);
      rand = new Random(bspInputSplit.getSplitIndex());
      if (getConf().reuseEdgeObjects()) {
        reusableEdge = getConf().createReusableEdge();
      }
      localEdgesHelper = new PseudoRandomIntNullLocalEdgesHelper(
          aggregateVertices, getConf());
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead;
    }

    @Override
    public Vertex<IntWritable, FloatWritable, NullWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<IntWritable, FloatWritable, NullWritable> vertex =
          getConf().createVertex();
      int vertexId = startingVertexId + verticesRead;
      OutEdges<IntWritable, NullWritable> edges =
          getConf().createOutEdges();
      edges.initialize(edgesPerVertex);
      destVertices.clear();
      for (int i = 0; i < edgesPerVertex; ++i) {
        int destVertexId;
        do {
          destVertexId = localEdgesHelper.generateDestVertex(vertexId, rand);
        } while (!destVertices.add(destVertexId));
        Edge<IntWritable, NullWritable> edge =
            (reusableEdge == null) ? getConf().createEdge() : reusableEdge;
        edge.getTargetVertexId().set(destVertexId);
        edges.add(edge);
      }
      vertex.initialize(
          new IntWritable(vertexId), new FloatWritable(1.0f), edges);
      ++verticesRead;
      return vertex;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      return verticesRead * 100.0f / totalSplitVertices;
    }
  }
}
