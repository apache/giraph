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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Generates a random Watts-Strogatz graph by re-wiring a ring lattice.
 * The resulting graph is a random graph with high clustering coefficient
 * and low average path length. The graph has these two characteristics that
 * are typical of small-world scale-free graphs, however the degree
 * distribution is more similar to a random graph.
 * It supports a seed for pseudo-random generation.
 */
public class WattsStrogatzVertexInputFormat extends
  VertexInputFormat<LongWritable, DoubleWritable, DoubleWritable> {
  /** The number of vertices in the graph */
  private static final String AGGREGATE_VERTICES =
      "wattsStrogatz.aggregateVertices";
  /** The number of outgoing edges per vertex */
  private static final String EDGES_PER_VERTEX =
      "wattsStrogatz.edgesPerVertex";
  /** The probability to re-wire an outgoing edge from the ring lattice */
  private static final String BETA =
      "wattsStrogatz.beta";
  /** The seed to generate random values for pseudo-randomness */
  private static final String SEED =
      "wattsStrogatz.seed";

  @Override
  public void checkInputSpecs(Configuration conf) { }

  @Override
  public final List<InputSplit> getSplits(final JobContext context,
      final int minSplitCountHint) throws IOException, InterruptedException {
    return PseudoRandomUtils.getSplits(minSplitCountHint);
  }

  @Override
  public VertexReader<LongWritable, DoubleWritable, DoubleWritable>
  createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new WattsStrogatzVertexReader();
  }

  /**
   * Vertex reader used to generate the graph
   */
  private static class WattsStrogatzVertexReader extends
    VertexReader<LongWritable, DoubleWritable, DoubleWritable> {
    /** the re-wiring probability */
    private float beta = 0;
    /** The total number of vertices */
    private long aggregateVertices = 0;
    /** The starting vertex id for this split */
    private long startingVertexId = -1;
    /** The number of vertices read so far */
    private long verticesRead = 0;
    /** The total number of vertices in the split */
    private long totalSplitVertices = -1;
    /** the total number of outgoing edges per vertex */
    private int edgesPerVertex = -1;
    /** The target ids of the outgoing edges */
    private final LongSet destVertices = new LongOpenHashSet();
    /** The random values generator */
    private Random rnd;
    /** The reusable edge */
    private ReusableEdge<LongWritable, DoubleWritable> reusableEdge = null;

    /**
     * Default constructor
     */
    public WattsStrogatzVertexReader() { }

    @Override
    public void initialize(InputSplit inputSplit,
        TaskAttemptContext context) throws IOException {
      beta = getConf().getFloat(
          BETA, 0.0f);
      aggregateVertices = getConf().getLong(
          AGGREGATE_VERTICES, 0);
      BspInputSplit bspInputSplit = (BspInputSplit) inputSplit;
      long extraVertices = aggregateVertices % bspInputSplit.getNumSplits();
      totalSplitVertices = aggregateVertices / bspInputSplit.getNumSplits();
      if (bspInputSplit.getSplitIndex() < extraVertices) {
        ++totalSplitVertices;
      }
      startingVertexId = bspInputSplit.getSplitIndex() *
          (aggregateVertices / bspInputSplit.getNumSplits()) +
          Math.min(bspInputSplit.getSplitIndex(), extraVertices);
      edgesPerVertex = getConf().getInt(
          EDGES_PER_VERTEX, 0);
      if (getConf().reuseEdgeObjects()) {
        reusableEdge = getConf().createReusableEdge();
      }
      int seed = getConf().getInt(SEED, -1);
      if (seed != -1) {
        rnd = new Random(seed);
      } else {
        rnd = new Random();
      }
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead;
    }

    /**
     * Return a long value uniformly distributed between 0 (inclusive) and n.
     *
     * @param n the upper bound for the random long value
     * @return the random value
     */
    private long nextLong(long n) {
      long bits;
      long val;
      do {
        bits = (rnd.nextLong() << 1) >>> 1;
        val = bits % n;
      } while (bits - val + (n - 1) < 0L);
      return val;
    }

    /**
     * Get a destination id that is not already in the neighborhood and
     * that is not the vertex itself (no self-loops). For the second condition
     * it expects destVertices to contain the own id already.
     *
     * @return the destination vertex id
     */
    private long getRandomDestination() {
      long randomId;
      do {
        randomId = nextLong(aggregateVertices);
      } while (!destVertices.add(randomId));
      return randomId;
    }

    @Override
    public Vertex<LongWritable, DoubleWritable, DoubleWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex =
          getConf().createVertex();
      long vertexId = startingVertexId + verticesRead;
      OutEdges<LongWritable, DoubleWritable> edges =
          getConf().createOutEdges();
      edges.initialize(edgesPerVertex);
      destVertices.clear();
      destVertices.add(vertexId);
      long destVertexId = vertexId - edgesPerVertex / 2;
      if (destVertexId < 0) {
        destVertexId = aggregateVertices + destVertexId;
      }
      for (int i = 0; i < edgesPerVertex + 1; ++i) {
        if (destVertexId != vertexId) {
          Edge<LongWritable, DoubleWritable> edge =
              (reusableEdge == null) ? getConf().createEdge() : reusableEdge;
          edge.getTargetVertexId().set(
              rnd.nextFloat() < beta ? getRandomDestination() : destVertexId);
          edge.getValue().set(rnd.nextDouble());
          edges.add(edge);
        }
        destVertexId = (destVertexId + 1) % aggregateVertices;
      }
      vertex.initialize(new LongWritable(vertexId),
          new DoubleWritable(rnd.nextDouble()), edges);
      ++verticesRead;
      return vertex;
    }

    @Override
    public void close() throws IOException { }

    @Override
    public float getProgress() throws IOException {
      return verticesRead * 100.0f / totalSplitVertices;
    }
  }
}
