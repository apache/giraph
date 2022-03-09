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
package org.apache.giraph.debugger.examples;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

/**
 * Input format that loads the Bipartite graph for matching where each vertex
 * is a JSON array with three elements: vertex id, vertex value (ignored), and
 * an array of neighbor vertex ids.
 *
 * @param <V>
 */
public class BipartiteGraphInputFormat<V extends Writable>
  extends
  TextVertexInputFormat<LongWritable, V, NullWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new JsonBipartiteVertexReader();
  }

  /**
   * A JSON vertex reader for the BipartiteGraphInputFormat.
   */
  private class JsonBipartiteVertexReader
    extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException,
      IOException {
      return new JSONArray(line.toString());
    }

    @Override
    protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
      IOException {
      return new LongWritable(jsonVertex.getLong(0));
    }

    @Override
    protected V getValue(JSONArray jsonVertex) throws JSONException,
      IOException {
      // Ignoring jsonVertex.getJSONArray(1)
      return null;
    }

    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
      JSONArray jsonVertex) throws JSONException, IOException {
      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
      List<Edge<LongWritable, NullWritable>> edges = Lists
        .newArrayListWithCapacity(jsonEdgeArray.length());
      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        long neighbor = jsonEdgeArray.getLong(i);
        edges.add(EdgeFactory.create(new LongWritable(neighbor),
          NullWritable.get()));
      }
      return edges;
    }

  }

}
