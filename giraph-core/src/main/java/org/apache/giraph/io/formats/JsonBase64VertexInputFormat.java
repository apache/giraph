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

import com.google.common.collect.Lists;
import net.iharder.Base64;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 * Simple way to represent the structure of the graph with a JSON object.
 * The actual vertex ids, values, edges are stored by the
 * Writable serialized bytes that are Byte64 encoded.
 * Works with {@link JsonBase64VertexOutputFormat}
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class JsonBase64VertexInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends TextVertexInputFormat<I, V, E> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new JsonBase64VertexReader();
  }

  /**
   * Simple reader that supports {@link JsonBase64VertexInputFormat}
   */
  protected class JsonBase64VertexReader extends
    TextVertexReaderFromEachLineProcessed<JSONObject> {


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
      super.initialize(inputSplit, context);
    }

    @Override
    protected JSONObject preprocessLine(Text line) {
      try {
        return new JSONObject(line.toString());
      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "next: Failed to get the vertex", e);
      }
    }

    @Override
    protected I getId(JSONObject vertexObject) throws IOException {
      try {
        byte[] decodedWritable = Base64.decode(
            vertexObject.getString(JsonBase64VertexFormat.VERTEX_ID_KEY));
        DataInput input = new DataInputStream(
            new ByteArrayInputStream(decodedWritable));
        I vertexId = getConf().createVertexId();
        vertexId.readFields(input);
        return vertexId;
      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "next: Failed to get vertex id", e);
      }
    }

    @Override
    protected V getValue(JSONObject vertexObject) throws IOException {
      try {
        byte[] decodedWritable = Base64.decode(
            vertexObject.getString(JsonBase64VertexFormat.VERTEX_VALUE_KEY));
        DataInputStream input = new DataInputStream(
            new ByteArrayInputStream(decodedWritable));
        V vertexValue = getConf().createVertexValue();
        vertexValue.readFields(input);
        return vertexValue;
      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "next: Failed to get vertex value", e);
      }
    }

    @Override
    protected Iterable<Edge<I, E>> getEdges(JSONObject vertexObject) throws
    IOException {
      JSONArray edgeArray = null;
      try {
        edgeArray = vertexObject.getJSONArray(
          JsonBase64VertexFormat.EDGE_ARRAY_KEY);
      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "next: Failed to get edge array", e);
      }
      byte[] decodedWritable;
      List<Edge<I, E>> edges = Lists.newArrayListWithCapacity(
          edgeArray.length());
      for (int i = 0; i < edgeArray.length(); ++i) {
        try {
          decodedWritable = Base64.decode(edgeArray.getString(i));
        } catch (JSONException e) {
          throw new IllegalArgumentException(
            "next: Failed to get edge value", e);
        }
        DataInputStream input = new DataInputStream(
            new ByteArrayInputStream(decodedWritable));
        I targetVertexId = getConf().createVertexId();
        targetVertexId.readFields(input);
        E edgeValue = getConf().createEdgeValue();
        edgeValue.readFields(input);
        edges.add(EdgeFactory.create(targetVertexId, edgeValue));
      }
      return edges;
    }

  }

}
