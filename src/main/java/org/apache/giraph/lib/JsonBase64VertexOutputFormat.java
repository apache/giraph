/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.lib;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.commons.codec.binary.Base64;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
public class JsonBase64VertexOutputFormat<
        I extends WritableComparable, V extends Writable, E extends Writable>
        extends TextVertexOutputFormat<I, V, E>
        implements JsonBase64VertexFormat {
    /**
     * Simple writer that supports {@link JsonBase64VertexOutputFormat}
     *
     * @param <I> Vertex index value
     * @param <V> Vertex value
     * @param <E> Edge value
     */
    private static class JsonBase64VertexWriter<
            I extends WritableComparable, V extends Writable,
            E extends Writable> extends TextVertexWriter<I, V, E> {
        /**
         * Only constructor.  Requires the LineRecordWriter
         *
         * @param lineRecordWriter Line record writer to write to
         */
        public JsonBase64VertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<I, V, E, ?> vertex)
                throws IOException, InterruptedException {
            ByteArrayOutputStream outputStream =
                new ByteArrayOutputStream();
            DataOutput output = new DataOutputStream(outputStream);
            JSONObject vertexObject = new JSONObject();
            vertex.getVertexId().write(output);
            try {
                vertexObject.put(
                    VERTEX_ID_KEY,
                    Base64.encodeBase64String(outputStream.toByteArray()));
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "writerVertex: Failed to insert vertex id", e);
            }
            outputStream.reset();
            vertex.getVertexValue().write(output);
            try {
                vertexObject.put(
                    VERTEX_VALUE_KEY,
                    Base64.encodeBase64String(outputStream.toByteArray()));
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "writerVertex: Failed to insert vertex value", e);
            }
            JSONArray edgeArray = new JSONArray();
            for (Edge<I, E> edge :vertex.getOutEdgeMap().values()) {
                outputStream.reset();
                edge.write(output);
                edgeArray.put(Base64.encodeBase64String(outputStream.toByteArray()));
            }
            try {
                vertexObject.put(EDGE_ARRAY_KEY, edgeArray);
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "writerVertex: Failed to insert edge array", e);
            }
            getRecordWriter().write(new Text(vertexObject.toString()), null);
        }
    }

    @Override
    public VertexWriter<I, V, E> createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new JsonBase64VertexWriter<I, V, E>(
            textOutputFormat.getRecordWriter(context));
    }

}
