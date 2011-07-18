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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.commons.codec.binary.Base64;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
public class JsonBase64VertexInputFormat<
        I extends WritableComparable, V extends Writable, E extends Writable>
        extends TextVertexInputFormat<I, V, E> implements
        JsonBase64VertexFormat {
    /**
     * Simple reader that supports {@link JsonBase64VertexInputFormat}
     *
     * @param <I> Vertex index value
     * @param <V> Vertex value
     * @param <E> Edge value
     */
    private static class JsonBase64VertexReader<
            I extends WritableComparable, V extends Writable,
            E extends Writable> extends TextVertexReader<I, V, E> {
        /**
         * Only constructor.  Requires the LineRecordReader
         *
         * @param lineRecordReader Line record reader to read from
         */
        public JsonBase64VertexReader(
                RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        @Override
        public boolean next(MutableVertex<I, V, E, ?> vertex)
                throws IOException, InterruptedException {
            if (!getRecordReader().nextKeyValue()) {
                return false;
            }

            Configuration conf = getContext().getConfiguration();
            Text line = getRecordReader().getCurrentValue();
            JSONObject vertexObject;
            try {
                vertexObject = new JSONObject(line.toString());
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Failed to get the vertex", e);
            }
            DataInput input = null;
            byte[] decodedWritable = null;
            try {
                decodedWritable = Base64.decodeBase64(
                    vertexObject.getString(VERTEX_ID_KEY));
                input = new DataInputStream(
                    new ByteArrayInputStream(decodedWritable));
                I vertexId = BspUtils.<I>createVertexIndex(conf);
                vertexId.readFields(input);
                vertex.setVertexId(vertexId);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Failed to get vertex id", e);
            }
            try {
                decodedWritable = Base64.decodeBase64(
                    vertexObject.getString(VERTEX_VALUE_KEY));
                input = new DataInputStream(
                    new ByteArrayInputStream(decodedWritable));
                V vertexValue = BspUtils.<V>createVertexValue(conf);
                vertexValue.readFields(input);
                vertex.setVertexValue(vertexValue);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Failed to get vertex value", e);
            }
            JSONArray edgeArray = null;
            try {
                edgeArray = vertexObject.getJSONArray(EDGE_ARRAY_KEY);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Failed to get edge array", e);
            }
            for (int i = 0; i < edgeArray.length(); ++i) {
                try {
                    decodedWritable =
                        Base64.decodeBase64(edgeArray.getString(i));
                } catch (JSONException e) {
                    throw new IllegalArgumentException(
                        "next: Failed to get edge value", e);
                }
                input = new DataInputStream(
                    new ByteArrayInputStream(decodedWritable));
                Edge<I, E> edge = new Edge<I, E>();
                edge.setConf(getContext().getConfiguration());
                edge.readFields(input);
                vertex.addEdge(edge);
            }
            return true;
        }
    }

    @Override
    public VertexReader<I, V, E> createVertexReader(
            InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new JsonBase64VertexReader<I, V, E>(
            textInputFormat.createRecordReader(split, context));
    }
}
