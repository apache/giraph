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
package org.apache.giraph.format.hbase.edgemarker;

import org.apache.giraph.format.hbase.HBaseVertexOutputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
/*
 Test subclass for HBaseVertexOutputFormat
 */
public class TableEdgeOutputFormat
        extends HBaseVertexOutputFormat<Text, Text, Text> {


    public VertexWriter<Text, Text, Text>
    createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        RecordWriter<ImmutableBytesWritable, Writable> writer =
                tableOutputFormat.getRecordWriter(context);
        return new TableEdgeVertexWriter(writer);
    }

    /*
     For each vertex, write back to the configured table using
     the vertex id as the row key bytes.
     */
    public static class TableEdgeVertexWriter
            extends HBaseVertexWriter<Text, Text, Text> {

        private final byte[] CF = Bytes.toBytes("cf");
        private final byte[] PARENT =  Bytes.toBytes("parent");

        public TableEdgeVertexWriter(
                RecordWriter<ImmutableBytesWritable, Writable> writer) {
            super(writer);
        }
        /*
         Record the vertex value as a the value for a new qualifier 'parent'.
         */
        public void writeVertex(
                Vertex<Text, Text, Text, ?> vertex)
                throws IOException, InterruptedException {
              RecordWriter<ImmutableBytesWritable, Writable> writer = getRecordWriter();
              byte[] rowBytes = vertex.getId().getBytes();
              Put put = new Put(rowBytes);
              Text value = vertex.getValue();
              if(value.toString().length() > 0)   {
                 put.add(CF, PARENT, value.getBytes());
                 writer.write(new ImmutableBytesWritable(rowBytes), put);
              }
        }
    }
}
