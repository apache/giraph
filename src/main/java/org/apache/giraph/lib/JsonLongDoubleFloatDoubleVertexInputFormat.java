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
package org.apache.giraph.lib;

import org.apache.giraph.graph.VertexReader;
import org.json.JSONArray;
import org.json.JSONException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.google.common.collect.Maps;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import java.io.IOException;
import java.util.Map;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>double</code> vertex values and <code>float</code>
  * out-edge weights, and <code>double</code> message types,
  *  specified in JSON format.
  */
public class JsonLongDoubleFloatDoubleVertexInputFormat extends
  TextVertexInputFormat<LongWritable, DoubleWritable,
  FloatWritable, DoubleWritable> {

  @Override
  public VertexReader<LongWritable, DoubleWritable, FloatWritable,
    DoubleWritable> createVertexReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new JsonLongDoubleFloatDoubleVertexReader(
      textInputFormat.createRecordReader(split, context));
  }

 /**
  * VertexReader that features <code>double</code> vertex
  * values and <code>float</code> out-edge weights. The
  * files should be in the following JSON format:
  * JSONArray(<vertex id>, <vertex value>,
  *   JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
  * Here is an example with vertex id 1, vertex value 4.3, and two edges.
  * First edge has a destination vertex 2, edge value 2.1.
  * Second edge has a destination vertex 3, edge value 0.7.
  * [1,4.3,[[2,2.1],[3,0.7]]]
  */
  static class JsonLongDoubleFloatDoubleVertexReader extends
    TextVertexReader<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> {

  /**
    * Constructor with the line record reader.
    *
    * @param lineRecordReader Will read from this line.
    */
    public JsonLongDoubleFloatDoubleVertexReader(
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
}
