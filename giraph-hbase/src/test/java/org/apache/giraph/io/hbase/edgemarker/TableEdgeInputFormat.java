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
package org.apache.giraph.io.hbase.edgemarker;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.hbase.HBaseVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 *  Test subclass for HBaseVertexInputFormat. Reads a simple
 *  children qualifier to create an edge.
 */
public class TableEdgeInputFormat extends
    HBaseVertexInputFormat<Text, Text, Text> {

  private static final Logger LOG =
      Logger.getLogger(TableEdgeInputFormat.class);
  private static final Text uselessEdgeValue = new Text();

  @Override public void checkInputSpecs(Configuration conf) { }

  public VertexReader<Text, Text, Text>
  createVertexReader(InputSplit split,
                     TaskAttemptContext context) throws IOException {

    return new TableEdgeVertexReader(split, context);

  }

  /**
   * Uses the RecordReader to return Hbase rows
   */
  public static class TableEdgeVertexReader
      extends HBaseVertexReader<Text, Text, Text> {

    private final byte[] CF = Bytes.toBytes("cf");
    private final byte[] CHILDREN = Bytes.toBytes("children");

    public TableEdgeVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
      super(split, context);
    }

    @Override
    public boolean nextVertex() throws IOException,
        InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /**
     * For each row, create a vertex with the row ID as a text,
     * and it's 'children' qualifier as a single edge.
     */
    @Override
    public Vertex<Text, Text, Text>
    getCurrentVertex()
        throws IOException, InterruptedException {
      Result row = getRecordReader().getCurrentValue();
      Vertex<Text, Text, Text> vertex =
          getConf().createVertex();
      Text vertexId = new Text(Bytes.toString(row.getRow()));
      List<Edge<Text, Text>> edges = Lists.newLinkedList();
      String edge = Bytes.toString(row.getValue(CF, CHILDREN));
      Text vertexValue = new Text();
      Text edgeId = new Text(edge);
      edges.add(EdgeFactory.create(edgeId, uselessEdgeValue));
      vertex.initialize(vertexId, vertexValue, edges);

      return vertex;
    }
  }
}
