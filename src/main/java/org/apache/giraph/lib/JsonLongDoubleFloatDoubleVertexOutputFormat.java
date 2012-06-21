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

import org.apache.giraph.graph.VertexWriter;
import org.json.JSONArray;
import org.json.JSONException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.graph.BasicVertex;
import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>double</code> values and <code>float</code> out-edge weights
 */
public class JsonLongDoubleFloatDoubleVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, DoubleWritable,
  FloatWritable> {
  @Override
  public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
  createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    RecordWriter<Text, Text> recordWriter =
      textOutputFormat.getRecordWriter(context);
    return new JsonLongDoubleFloatDoubleVertexWriter(recordWriter);
  }

 /**
  * VertexWriter that supports vertices with <code>double</code>
  * values and <code>float</code> out-edge weights.
  */
  static class JsonLongDoubleFloatDoubleVertexWriter extends
    TextVertexWriter<LongWritable, DoubleWritable, FloatWritable> {

   /**
    * Vertex writer with the internal line writer.
    *
    * @param lineRecordWriter Wil actually be written to.
    */
    public JsonLongDoubleFloatDoubleVertexWriter(
      RecordWriter<Text, Text> lineRecordWriter) {
      super(lineRecordWriter);
    }

    @Override
    public void writeVertex(BasicVertex<LongWritable, DoubleWritable,
      FloatWritable, ?> vertex) throws IOException, InterruptedException {
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
}
