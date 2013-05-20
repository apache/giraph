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

import java.io.IOException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.accumulo.AccumuloVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/*
 Example subclass for writing vertices back to Accumulo.
 */
public class AccumuloEdgeOutputFormat
    extends AccumuloVertexOutputFormat<Text, Text, Text> {

  public VertexWriter<Text, Text, Text>
  createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    RecordWriter<Text, Mutation> writer =
        accumuloOutputFormat.getRecordWriter(context);
    String tableName = getConf().get(OUTPUT_TABLE);
    if(tableName == null)
      throw new IOException("Forgot to set table name " +
          "using AccumuloVertexOutputFormat.OUTPUT_TABLE");
    return new AccumuloEdgeVertexWriter(writer, tableName);
  }

  /*
  Wraps RecordWriter for writing Mutations back to the configured Accumulo Table.
   */
  public static class AccumuloEdgeVertexWriter
      extends AccumuloVertexWriter<Text, Text, Text> {

    private final Text CF = new Text("cf");
    private final Text PARENT =  new Text("parent");
    private Text tableName;

    public AccumuloEdgeVertexWriter(
        RecordWriter<Text, Mutation> writer, String tableName) {
      super(writer);
      this.tableName = new Text(tableName);
    }
    /*
     Write back a mutation that adds a qualifier for 'parent' containing the vertex value
     as the cell value. Assume the vertex ID corresponds to a key.
     */
    public void writeVertex(Vertex<Text, Text, Text> vertex)
        throws IOException, InterruptedException {
      RecordWriter<Text, Mutation> writer = getRecordWriter();
      Mutation mt = new Mutation(vertex.getId());
      mt.put(CF, PARENT, new Value(
          vertex.getValue().toString().getBytes()));
      writer.write(tableName, mt);
    }
  }
}
