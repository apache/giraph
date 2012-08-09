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

package org.apache.giraph.io;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
  /** Set the number of aggregate vertices. */
  public static final String AGGREGATE_VERTICES =
      "pseudoRandomVertexReader.aggregateVertices";
  /** Set the number of edges per vertex (pseudo-random destination). */
  public static final String EDGES_PER_VERTEX =
      "pseudoRandomVertexReader.edgesPerVertex";

  @Override
  public final List<InputSplit> getSplits(final JobContext context,
      final int numWorkers) throws IOException, InterruptedException {
    // This is meaningless, the PseudoRandomVertexReader will generate
    // all the test data
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    for (int i = 0; i < numWorkers; ++i) {
      inputSplitList.add(new BspInputSplit(i, numWorkers));
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
    private long edgesPerVertex = -1;
    /** BspInputSplit (used only for index). */
    private BspInputSplit bspInputSplit;
    /** Saved configuration */
    private Configuration configuration;

    /**
     * Default constructor for reflection.
     */
    public PseudoRandomVertexReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit,
        TaskAttemptContext context) throws IOException {
      configuration = context.getConfiguration();
      aggregateVertices =
        configuration.getLong(
          PseudoRandomVertexInputFormat.AGGREGATE_VERTICES, 0);
      if (aggregateVertices <= 0) {
        throw new IllegalArgumentException(
            PseudoRandomVertexInputFormat.AGGREGATE_VERTICES + " <= 0");
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
      edgesPerVertex = configuration.getLong(
        PseudoRandomVertexInputFormat.EDGES_PER_VERTEX, 0);
      if (edgesPerVertex <= 0) {
        throw new IllegalArgumentException(
          PseudoRandomVertexInputFormat.EDGES_PER_VERTEX + " <= 0");
      }
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead;
    }

    @Override
    public Vertex<LongWritable, DoubleWritable, DoubleWritable, M>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable, DoubleWritable, M>
      vertex = BspUtils.createVertex(configuration);
      long vertexId = startingVertexId + verticesRead;
      // Seed on the vertex id to keep the vertex data the same when
      // on different number of workers, but other parameters are the
      // same.
      Random rand = new Random(vertexId);
      DoubleWritable vertexValue = new DoubleWritable(rand.nextDouble());
      Map<LongWritable, DoubleWritable> edges =
          Maps.newHashMapWithExpectedSize((int) edgesPerVertex);
      for (long i = 0; i < edgesPerVertex; ++i) {
        LongWritable destVertexId = null;
        do {
          destVertexId =
            new LongWritable(Math.abs(rand.nextLong()) %
              aggregateVertices);
        } while (edges.containsKey(destVertexId));
        edges.put(destVertexId, new DoubleWritable(rand.nextDouble()));
      }
      vertex.initialize(new LongWritable(vertexId), vertexValue, edges, null);
      ++verticesRead;
      if (LOG.isDebugEnabled()) {
        LOG.debug("next: Return vertexId=" +
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
