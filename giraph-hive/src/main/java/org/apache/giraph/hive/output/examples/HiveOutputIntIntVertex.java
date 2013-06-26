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
package org.apache.giraph.hive.output.examples;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.output.SimpleVertexToHive;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Preconditions;

/**
 * VertexToHive that writes Vertexes with integer IDs and integer values
 */
public class HiveOutputIntIntVertex extends SimpleVertexToHive<IntWritable,
    IntWritable, NullWritable> {
  @Override public void checkOutput(HiveOutputDescription outputDesc,
      HiveTableSchema schema, HiveWritableRecord emptyRecord) {
    Preconditions.checkArgument(schema.columnType(0) == HiveType.LONG);
    Preconditions.checkArgument(schema.columnType(1) == HiveType.LONG);
  }

  @Override public void fillRecord(
      Vertex<IntWritable, IntWritable, NullWritable> vertex,
      HiveWritableRecord record) {
    record.set(0, (long) vertex.getId().get());
    record.set(1, (long) vertex.getValue().get());
  }
}
