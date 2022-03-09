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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>double</code> values and <code>float</code> out-edge weights
 */
public class JsonLongDoubleFloatDoubleVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, DoubleWritable,
  FloatWritable> {

  @Override
  public TextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new JsonLongDoubleFloatDoubleVertexWriter();
  }

 /**
  * VertexWriter that supports vertices with <code>double</code>
  * values and <code>float</code> out-edge weights.
  */
  private class JsonLongDoubleFloatDoubleVertexWriter extends
    TextVertexWriterToEachLine {
    @Override
    public Text convertVertexToLine(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex
    ) throws IOException {
      JSONArray jsonVertex = new JSONArray();
      try {
        jsonVertex.put(vertex.getId().get());
        jsonVertex.put(vertex.getValue().get());
        JSONArray jsonEdgeArray = new JSONArray();
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
          JSONArray jsonEdge = new JSONArray();
          jsonEdge.put(edge.getTargetVertexId().get());
          jsonEdge.put(edge.getValue().get());
          jsonEdgeArray.put(jsonEdge);
        }
        jsonVertex.put(jsonEdgeArray);
      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "writeVertex: Couldn't write vertex " + vertex);
      }
      return new Text(jsonVertex.toString());
    }
  }
}
