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
package org.apache.giraph.io.accumulo.edgemarker;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.accumulo.AccumuloVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/*
 Example subclass which reads in Key/Value pairs to construct vertex objects.
 */
public class AccumuloEdgeInputFormat
    extends AccumuloVertexInputFormat<Text, Text, Text> {
  @Override public void checkInputSpecs(Configuration conf) { }

  private static final Text uselessEdgeValue = new Text();
  public VertexReader<Text, Text, Text>
  createVertexReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    try {

      return new AccumuloEdgeVertexReader(
          accumuloInputFormat.createRecordReader(split, context)) {
      };
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

  }
  /*
      Reader takes Key/Value pairs from the underlying input format.
   */
  public static class AccumuloEdgeVertexReader
      extends AccumuloVertexReader<Text, Text, Text> {

    public static final Pattern commaPattern = Pattern.compile("[,]");

    public AccumuloEdgeVertexReader(RecordReader<Key, Value> recordReader) {
      super(recordReader);
    }


    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /*
   Each Key/Value contains the information needed to construct the vertices.
     */
    public Vertex<Text, Text, Text> getCurrentVertex()
        throws IOException, InterruptedException {
      Key key = getRecordReader().getCurrentKey();
      Value value = getRecordReader().getCurrentValue();
      Vertex<Text, Text, Text> vertex =
          getConfiguration().createVertex();
      Text vertexId = key.getRow();
      List<Edge<Text, Text>> edges = Lists.newLinkedList();
      String edge = new String(value.get());
      Text edgeId = new Text(edge);
      edges.add(EdgeFactory.create(edgeId, uselessEdgeValue));
      vertex.initialize(vertexId, new Text(), edges);

      return vertex;
    }
  }
}
