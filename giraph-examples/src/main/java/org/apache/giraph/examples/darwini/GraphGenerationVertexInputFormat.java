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

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.PseudoRandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * This is not actually an input in the sense that it doesn't
 * read anything from HDFS/Hive or whatever, instead it generates
 * vertices on the fly.
 * Generated vertices have ids distributed from 0 to N - 1, consecutively.
 * Each vertex gets a random degree and clustering coefficient drawn
 * from the specified distribution.
 */
public class GraphGenerationVertexInputFormat extends
  VertexInputFormat<LongWritable, VertexData, NullWritable> {

  @Override
  public void checkInputSpecs(Configuration conf) { }

  @Override
  public final List<InputSplit> getSplits(final JobContext context,
      final int minSplitCountHint) throws IOException, InterruptedException {
    return PseudoRandomUtils.getSplits(minSplitCountHint);
  }

  @Override
  public VertexReader<LongWritable, VertexData, NullWritable>
  createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new GraphGenerationVertexReader();
  }

  /**
   * Vertex reader used to generate the graph
   */
  public static class GraphGenerationVertexReader extends
    VertexReader<LongWritable, VertexData, NullWritable> {
    /** The total number of vertices */
    private long aggregateVertices = 0;
    /** The starting vertex id for this split */
    private long startingVertexId = -1;
    /** The number of vertices read so far */
    private long verticesRead = 0;
    /** The total number of vertices in the split */
    private long totalSplitVertices = -1;
    /** degree and clustering coefficient distributions */
    private GeneratorUtils distributions;

    /**
     * Default constructor
     */
    public GraphGenerationVertexReader() { }

    @Override
    public void initialize(InputSplit inputSplit,
        TaskAttemptContext context) throws IOException {
      distributions = new GeneratorUtils(getConf());
      aggregateVertices = Constants.AGGREGATE_VERTICES.get(getConf());

      BspInputSplit bspInputSplit = (BspInputSplit) inputSplit;
      long extraVertices = aggregateVertices % bspInputSplit.getNumSplits();
      totalSplitVertices = aggregateVertices / bspInputSplit.getNumSplits();
      if (bspInputSplit.getSplitIndex() < extraVertices) {
        ++totalSplitVertices;
      }
      startingVertexId = bspInputSplit.getSplitIndex() *
          (aggregateVertices / bspInputSplit.getNumSplits()) +
          Math.min(bspInputSplit.getSplitIndex(), extraVertices);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead;
    }


    @Override
    public Vertex<LongWritable, VertexData, NullWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, VertexData, NullWritable> vertex =
          getConf().createVertex();
      long vertexId = startingVertexId + verticesRead;

      OutEdges<LongWritable, NullWritable> edges =
          getConf().createOutEdges();
      edges.initialize();

      int degree = distributions.randomDegree();
      float cc = distributions.randomCC(degree);
      int inCountryDegree = distributions.randomInSuperCommunityDegree(degree);
      vertex.initialize(new LongWritable(vertexId),
          new VertexData(degree, inCountryDegree, cc, -1), edges);

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
