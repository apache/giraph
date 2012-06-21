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

package org.apache.giraph.examples;

import com.google.common.collect.Maps;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.LongDoubleFloatDoubleVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
public class SimplePageRankVertex extends LongDoubleFloatDoubleVertex {
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 30;
  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(SimplePageRankVertex.class);

  @Override
  public void compute(Iterator<DoubleWritable> msgIterator) {
    LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator("sum");
    DoubleMinAggregator minAggreg = (DoubleMinAggregator) getAggregator("min");
    DoubleMaxAggregator maxAggreg = (DoubleMaxAggregator) getAggregator("max");
    if (getSuperstep() >= 1) {
      double sum = 0;
      while (msgIterator.hasNext()) {
        sum += msgIterator.next().get();
      }
      DoubleWritable vertexValue =
          new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
      setVertexValue(vertexValue);
      maxAggreg.aggregate(vertexValue);
      minAggreg.aggregate(vertexValue);
      sumAggreg.aggregate(1L);
      LOG.info(getVertexId() + ": PageRank=" + vertexValue +
          " max=" + maxAggreg.getAggregatedValue() +
          " min=" + minAggreg.getAggregatedValue());
    }

    if (getSuperstep() < MAX_SUPERSTEPS) {
      long edges = getNumOutEdges();
      sendMsgToAllEdges(
          new DoubleWritable(getVertexValue().get() / edges));
    } else {
      voteToHalt();
    }
  }

  /**
   * Worker context used with {@link SimplePageRankVertex}.
   */
  public static class SimplePageRankVertexWorkerContext extends
      WorkerContext {
    /** Final max value for verification for local jobs */
    private static double FINAL_MAX;
    /** Final min value for verification for local jobs */
    private static double FINAL_MIN;
    /** Final sum value for verification for local jobs */
    private static long FINAL_SUM;

    public static double getFinalMax() {
      return FINAL_MAX;
    }

    public static double getFinalMin() {
      return FINAL_MIN;
    }

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
      registerAggregator("sum", LongSumAggregator.class);
      registerAggregator("min", DoubleMinAggregator.class);
      registerAggregator("max", DoubleMaxAggregator.class);
    }

    @Override
    public void postApplication() {

      LongSumAggregator sumAggreg =
          (LongSumAggregator) getAggregator("sum");
      DoubleMinAggregator minAggreg =
          (DoubleMinAggregator) getAggregator("min");
      DoubleMaxAggregator maxAggreg =
          (DoubleMaxAggregator) getAggregator("max");

      FINAL_SUM = sumAggreg.getAggregatedValue().get();
      FINAL_MAX = maxAggreg.getAggregatedValue().get();
      FINAL_MIN = minAggreg.getAggregatedValue().get();

      LOG.info("aggregatedNumVertices=" + FINAL_SUM);
      LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
      LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
    }

    @Override
    public void preSuperstep() {

      LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator("sum");
      DoubleMinAggregator minAggreg =
          (DoubleMinAggregator) getAggregator("min");
      DoubleMaxAggregator maxAggreg =
          (DoubleMaxAggregator) getAggregator("max");

      if (getSuperstep() >= 3) {
        LOG.info("aggregatedNumVertices=" +
            sumAggreg.getAggregatedValue() +
            " NumVertices=" + getNumVertices());
        if (sumAggreg.getAggregatedValue().get() != getNumVertices()) {
          throw new RuntimeException("wrong value of SumAggreg: " +
              sumAggreg.getAggregatedValue() + ", should be: " +
              getNumVertices());
        }
        DoubleWritable maxPagerank = maxAggreg.getAggregatedValue();
        LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
        DoubleWritable minPagerank = minAggreg.getAggregatedValue();
        LOG.info("aggregatedMinPageRank=" + minPagerank.get());
      }
      useAggregator("sum");
      useAggregator("min");
      useAggregator("max");
      sumAggreg.setAggregatedValue(0L);
    }

    @Override
    public void postSuperstep() { }
  }

  /**
   * Simple VertexReader that supports {@link SimplePageRankVertex}
   */
  public static class SimplePageRankVertexReader extends
      GeneratedVertexReader<LongWritable, DoubleWritable, FloatWritable,
      DoubleWritable> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(SimplePageRankVertexReader.class);

    /**
     * Constructor.
     */
    public SimplePageRankVertexReader() {
      super();
    }

    @Override
    public boolean nextVertex() {
      return totalRecords > recordsRead;
    }

    @Override
    public BasicVertex<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable>
    getCurrentVertex() throws IOException {
      BasicVertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
      vertex = BspUtils.createVertex(configuration);

      LongWritable vertexId = new LongWritable(
          (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
      DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
      long destVertexId =
          (vertexId.get() + 1) %
          (inputSplit.getNumSplits() * totalRecords);
      float edgeValue = vertexId.get() * 100f;
      Map<LongWritable, FloatWritable> edges = Maps.newHashMap();
      edges.put(new LongWritable(destVertexId), new FloatWritable(edgeValue));
      vertex.initialize(vertexId, vertexValue, edges, null);
      ++recordsRead;
      if (LOG.isInfoEnabled()) {
        LOG.info("next: Return vertexId=" + vertex.getVertexId().get() +
            ", vertexValue=" + vertex.getVertexValue() +
            ", destinationId=" + destVertexId + ", edgeValue=" + edgeValue);
      }
      return vertex;
    }
  }

  /**
   * Simple VertexInputFormat that supports {@link SimplePageRankVertex}
   */
  public static class SimplePageRankVertexInputFormat extends
      GeneratedVertexInputFormat<LongWritable,
        DoubleWritable, FloatWritable, DoubleWritable> {
    @Override
    public VertexReader<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> createVertexReader(InputSplit split,
      TaskAttemptContext context)
      throws IOException {
      return new SimplePageRankVertexReader();
    }
  }

  /**
   * Simple VertexWriter that supports {@link SimplePageRankVertex}
   */
  public static class SimplePageRankVertexWriter extends
      TextVertexWriter<LongWritable, DoubleWritable, FloatWritable> {
    /**
     * Constructor with line writer.
     *
     * @param lineRecordWriter Line writer that will do the writing.
     */
    public SimplePageRankVertexWriter(
        RecordWriter<Text, Text> lineRecordWriter) {
      super(lineRecordWriter);
    }

    @Override
    public void writeVertex(
      BasicVertex<LongWritable, DoubleWritable, FloatWritable, ?> vertex)
      throws IOException, InterruptedException {
      getRecordWriter().write(
          new Text(vertex.getVertexId().toString()),
          new Text(vertex.getVertexValue().toString()));
    }
  }

  /**
   * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
   */
  public static class SimplePageRankVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {
    @Override
    public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
    createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      RecordWriter<Text, Text> recordWriter =
          textOutputFormat.getRecordWriter(context);
      return new SimplePageRankVertexWriter(recordWriter);
    }
  }
}
