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

import org.apache.giraph.debugger.examples.bipartitematching.VertexValue;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;

/**
 * Output format that dumps the Bipartite graph after matching where each
 * vertex is a JSON array of three elements: vertex id, vertex value, and an
 * array of neighbor vertex ids.
 */
public class BipartiteGraphOutputFormat
  extends
  TextVertexOutputFormat<LongWritable,
  VertexValue, NullWritable> {

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
    return new BipartiteVertexWriter();
  }

  /**
   * A JSON vertex writer for the BipartiteGraphOutputFormat.
   */
  private class BipartiteVertexWriter extends TextVertexWriterToEachLine {
    @Override
    public Text convertVertexToLine(
      Vertex<LongWritable, VertexValue,
      NullWritable> vertex)
      throws IOException {
      JSONArray jsonVertex = new JSONArray();
      jsonVertex.put(vertex.getId().get());
      jsonVertex.put(vertex.getValue());
      JSONArray jsonEdgeArray = new JSONArray();
      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
        jsonEdgeArray.put(edge.getTargetVertexId().get());
      }
      jsonVertex.put(jsonEdgeArray);
      return new Text(jsonVertex.toString());
    }
  }
}
