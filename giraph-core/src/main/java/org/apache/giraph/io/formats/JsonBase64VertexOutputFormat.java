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

import net.iharder.Base64;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Simple way to represent the structure of the graph with a JSON object.
 * The actual vertex ids, values, edges are stored by the
 * Writable serialized bytes that are Byte64 encoded.
 * Works with {@link JsonBase64VertexInputFormat}
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class JsonBase64VertexOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    TextVertexOutputFormat<I, V, E> {

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
    return new JsonBase64VertexWriter();
  }

  /**
   * Simple writer that supports {@link JsonBase64VertexOutputFormat}
   */
  protected class JsonBase64VertexWriter extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(Vertex<I, V, E> vertex)
      throws IOException {
      ByteArrayOutputStream outputStream =
          new ByteArrayOutputStream();
      DataOutput output = new DataOutputStream(outputStream);
      JSONObject vertexObject = new JSONObject();
      vertex.getId().write(output);
      try {
        vertexObject.put(
          JsonBase64VertexFormat.VERTEX_ID_KEY,
          Base64.encodeBytes(outputStream.toByteArray()));
      } catch (JSONException e) {
        throw new IllegalStateException(
            "writerVertex: Failed to insert vertex id", e);
      }
      outputStream.reset();
      vertex.getValue().write(output);
      try {
        vertexObject.put(
          JsonBase64VertexFormat.VERTEX_VALUE_KEY,
          Base64.encodeBytes(outputStream.toByteArray()));
      } catch (JSONException e) {
        throw new IllegalStateException(
            "writerVertex: Failed to insert vertex value", e);
      }
      JSONArray edgeArray = new JSONArray();
      for (Edge<I, E> edge : vertex.getEdges()) {
        outputStream.reset();
        edge.getTargetVertexId().write(output);
        edge.getValue().write(output);
        edgeArray.put(Base64.encodeBytes(outputStream.toByteArray()));
      }
      try {
        vertexObject.put(
          JsonBase64VertexFormat.EDGE_ARRAY_KEY,
          edgeArray);
      } catch (JSONException e) {
        throw new IllegalStateException(
            "writerVertex: Failed to insert edge array", e);
      }
      return new Text(vertexObject.toString());
    }

  }

}
