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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class SimpleShortestPathsVertex extends
    EdgeListVertex<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> implements Tool {
  /** The shortest paths id */
  public static final String SOURCE_ID = "SimpleShortestPathsVertex.sourceId";
  /** Default shortest paths id */
  public static final long SOURCE_ID_DEFAULT = 1;
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleShortestPathsVertex.class);
  /** Configuration */
  private Configuration conf;

  /**
   * Is this vertex the source id?
   *
   * @return True if the source id
   */
  private boolean isSource() {
    return getVertexId().get() ==
        getContext().getConfiguration().getLong(SOURCE_ID,
            SOURCE_ID_DEFAULT);
  }

  @Override
  public void compute(Iterator<DoubleWritable> msgIterator) {
    if (getSuperstep() == 0) {
      setVertexValue(new DoubleWritable(Double.MAX_VALUE));
    }
    double minDist = isSource() ? 0d : Double.MAX_VALUE;
    while (msgIterator.hasNext()) {
      minDist = Math.min(minDist, msgIterator.next().get());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + getVertexId() + " got minDist = " + minDist +
          " vertex value = " + getVertexValue());
    }
    if (minDist < getVertexValue().get()) {
      setVertexValue(new DoubleWritable(minDist));
      for (LongWritable targetVertexId : this) {
        FloatWritable edgeValue = getEdgeValue(targetVertexId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + getVertexId() + " sent to " +
              targetVertexId + " = " +
              (minDist + edgeValue.get()));
        }
        sendMsg(targetVertexId,
            new DoubleWritable(minDist + edgeValue.get()));
      }
    }
    voteToHalt();
  }

  /**
   * VertexInputFormat that supports {@link SimpleShortestPathsVertex}
   */
  public static class SimpleShortestPathsVertexInputFormat extends
      TextVertexInputFormat<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> {
    @Override
    public VertexReader<LongWritable, DoubleWritable, FloatWritable,
    DoubleWritable> createVertexReader(InputSplit split,
      TaskAttemptContext context)
      throws IOException {
      return new SimpleShortestPathsVertexReader(
          textInputFormat.createRecordReader(split, context));
    }
  }

  /**
   * VertexReader that supports {@link SimpleShortestPathsVertex}.  In this
   * case, the edge values are not used.  The files should be in the
   * following JSON format:
   * JSONArray(<vertex id>, <vertex value>,
   *           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
   * Here is an example with vertex id 1, vertex value 4.3, and two edges.
   * First edge has a destination vertex 2, edge value 2.1.
   * Second edge has a destination vertex 3, edge value 0.7.
   * [1,4.3,[[2,2.1],[3,0.7]]]
   */
  public static class SimpleShortestPathsVertexReader extends
      TextVertexReader<LongWritable, DoubleWritable,
      FloatWritable, DoubleWritable> {

    /**
     * Constructor with the line record reader.
     *
     * @param lineRecordReader Will read from this line.
     */
    public SimpleShortestPathsVertexReader(
        RecordReader<LongWritable, Text> lineRecordReader) {
      super(lineRecordReader);
    }

    @Override
    public BasicVertex<LongWritable, DoubleWritable, FloatWritable,
    DoubleWritable> getCurrentVertex()
      throws IOException, InterruptedException {
      BasicVertex<LongWritable, DoubleWritable, FloatWritable,
      DoubleWritable> vertex =
        BspUtils.<LongWritable, DoubleWritable, FloatWritable,
          DoubleWritable>createVertex(getContext().getConfiguration());

      Text line = getRecordReader().getCurrentValue();
      try {
        JSONArray jsonVertex = new JSONArray(line.toString());
        LongWritable vertexId = new LongWritable(jsonVertex.getLong(0));
        DoubleWritable vertexValue =
            new DoubleWritable(jsonVertex.getDouble(1));
        Map<LongWritable, FloatWritable> edges = Maps.newHashMap();
        JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
        for (int i = 0; i < jsonEdgeArray.length(); ++i) {
          JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
          edges.put(new LongWritable(jsonEdge.getLong(0)),
              new FloatWritable((float) jsonEdge.getDouble(1)));
        }
        vertex.initialize(vertexId, vertexValue, edges, null);
      } catch (JSONException e) {
        throw new IllegalArgumentException(
            "next: Couldn't get vertex from line " + line, e);
      }
      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }

  /**
   * VertexOutputFormat that supports {@link SimpleShortestPathsVertex}
   */
  public static class SimpleShortestPathsVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, DoubleWritable,
      FloatWritable> {
    @Override
    public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
    createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      RecordWriter<Text, Text> recordWriter =
          textOutputFormat.getRecordWriter(context);
      return new SimpleShortestPathsVertexWriter(recordWriter);
    }
  }

  /**
   * VertexWriter that supports {@link SimpleShortestPathsVertex}
   */
  public static class SimpleShortestPathsVertexWriter extends
      TextVertexWriter<LongWritable, DoubleWritable, FloatWritable> {
    /**
     * Vertex writer with the internal line writer.
     *
     * @param lineRecordWriter Wil actually be written to.
     */
    public SimpleShortestPathsVertexWriter(
        RecordWriter<Text, Text> lineRecordWriter) {
      super(lineRecordWriter);
    }

    @Override
    public void writeVertex(BasicVertex<LongWritable, DoubleWritable,
      FloatWritable, ?> vertex)
      throws IOException, InterruptedException {
      JSONArray jsonVertex = new JSONArray();
      try {
        jsonVertex.put(vertex.getVertexId().get());
        jsonVertex.put(vertex.getVertexValue().get());
        JSONArray jsonEdgeArray = new JSONArray();
        for (LongWritable targetVertexId : vertex) {
          JSONArray jsonEdge = new JSONArray();
          jsonEdge.put(targetVertexId.get());
          jsonEdge.put(vertex.getEdgeValue(targetVertexId).get());
          jsonEdgeArray.put(jsonEdge);
        }
        jsonVertex.put(jsonEdgeArray);
      } catch (JSONException e) {
        throw new IllegalArgumentException(
            "writeVertex: Couldn't write vertex " + vertex);
      }
      getRecordWriter().write(new Text(jsonVertex.toString()), null);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] argArray) throws Exception {
    Preconditions.checkArgument(argArray.length == 4,
        "run: Must have 4 arguments <input path> <output path> " +
        "<source vertex id> <# of workers>");

    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.setVertexClass(getClass());
    job.setVertexInputFormatClass(
        SimpleShortestPathsVertexInputFormat.class);
    job.setVertexOutputFormatClass(
        SimpleShortestPathsVertexOutputFormat.class);
    FileInputFormat.addInputPath(job.getInternalJob(),
                                 new Path(argArray[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(),
                                   new Path(argArray[1]));
    job.getConfiguration().setLong(SimpleShortestPathsVertex.SOURCE_ID,
        Long.parseLong(argArray[2]));
    job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
        Integer.parseInt(argArray[3]),
        100.0f);

    return job.run(true) ? 0 : -1;
  }

  /**
   * Can be used for command line execution.
   *
   * @param args Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SimpleShortestPathsVertex(), args));
  }
}
