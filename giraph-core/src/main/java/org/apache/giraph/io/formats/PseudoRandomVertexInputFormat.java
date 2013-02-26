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
import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.VertexEdges;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * This VertexInputFormat is meant for large scale testing.  It allows the user
 * to create an input data source that a variable number of aggregate vertices
 * and edges per vertex that is repeatable for the exact same parameter
 * (pseudo-random).
 *
 * @param <M> Message data
 */
public class PseudoRandomVertexInputFormat<M extends Writable> extends
    VertexInputFormat<LongWritable, DoubleWritable, DoubleWritable, M> {
  @Override
  public final List<InputSplit> getSplits(final JobContext context,
      final int minSplitCountHint) throws IOException, InterruptedException {
    // This is meaningless, the PseudoRandomVertexReader will generate
    // all the test data
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    for (int i = 0; i < minSplitCountHint; ++i) {
      inputSplitList.add(new BspInputSplit(i, minSplitCountHint));
    }
    return inputSplitList;
  }

  @Override
  public VertexReader<LongWritable, DoubleWritable, DoubleWritable, M>
  createVertexReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new PseudoRandomVertexReader<M>();
  }

  /**
   * Used by {@link PseudoRandomVertexInputFormat} to read
   * pseudo-randomly generated data.
   */
  private static class PseudoRandomVertexReader<M extends Writable> implements
      VertexReader<LongWritable, DoubleWritable, DoubleWritable, M> {
    /** Logger. */
    private static final Logger LOG =
        Logger.getLogger(PseudoRandomVertexReader.class);
    /** Starting vertex id. */
    private long startingVertexId = -1;
    /** Vertices read so far. */
    private long verticesRead = 0;
    /** Total vertices to read (on this split alone). */
    private long totalSplitVertices = -1;
    /** Aggregate vertices (all input splits). */
    private long aggregateVertices = -1;
    /** Edges per vertex. */
    private int edgesPerVertex = -1;
    /** BspInputSplit (used only for index). */
    private BspInputSplit bspInputSplit;
    /** Saved configuration */
    private ImmutableClassesGiraphConfiguration configuration;
    /** Helper for generating pseudo-random local edges. */
    private PseudoRandomLocalEdgesHelper localEdgesHelper;

    /**
     * Default constructor for reflection.
     */
    public PseudoRandomVertexReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit,
        TaskAttemptContext context) throws IOException {
      configuration = new ImmutableClassesGiraphConfiguration(
          context.getConfiguration());
      aggregateVertices =
        configuration.getLong(
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
      edgesPerVertex = configuration.getInt(
          PseudoRandomInputFormatConstants.EDGES_PER_VERTEX, 0);
      if (edgesPerVertex <= 0) {
        throw new IllegalArgumentException(
          PseudoRandomInputFormatConstants.EDGES_PER_VERTEX + " <= 0");
      }
      float minLocalEdgesRatio = configuration.getFloat(
          PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO,
          PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO_DEFAULT);
      localEdgesHelper = new PseudoRandomLocalEdgesHelper(aggregateVertices,
          minLocalEdgesRatio, configuration);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead;
    }

    @Override
    public Vertex<LongWritable, DoubleWritable, DoubleWritable, M>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable, DoubleWritable, M>
      vertex = configuration.createVertex();
      long vertexId = startingVertexId + verticesRead;
      // Seed on the vertex id to keep the vertex data the same when
      // on different number of workers, but other parameters are the
      // same.
      Random rand = new Random(vertexId);
      DoubleWritable vertexValue = new DoubleWritable(rand.nextDouble());
      // In order to save memory and avoid copying, we add directly to a
      // VertexEdges instance.
      VertexEdges<LongWritable, DoubleWritable> edges =
          configuration.createAndInitializeVertexEdges(edgesPerVertex);
      Set<LongWritable> destVertices = Sets.newHashSet();
      for (long i = 0; i < edgesPerVertex; ++i) {
        LongWritable destVertexId = new LongWritable();
        do {
          destVertexId.set(
              localEdgesHelper.generateDestVertex(vertexId, rand));
        } while (destVertices.contains(destVertexId));
        edges.add(EdgeFactory.create(destVertexId,
            new DoubleWritable(rand.nextDouble())));
        destVertices.add(destVertexId);
      }
      vertex.initialize(new LongWritable(vertexId), vertexValue, edges);
      ++verticesRead;
      if (LOG.isTraceEnabled()) {
        LOG.trace("next: Return vertexId=" +
            vertex.getId().get() +
            ", vertexValue=" + vertex.getValue() +
            ", edges=" + vertex.getEdges());
      }
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
