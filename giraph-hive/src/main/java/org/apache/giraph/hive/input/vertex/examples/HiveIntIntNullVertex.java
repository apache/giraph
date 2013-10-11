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
package org.apache.giraph.hive.input.vertex.examples;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.hive.common.HiveParsing;
import org.apache.giraph.hive.input.vertex.SimpleHiveToVertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Simple HiveToVertex that reads vertices with integer IDs, no vertex values,
 * and edges with no values.
 */
public class HiveIntIntNullVertex
    extends SimpleHiveToVertex<IntWritable, IntWritable, NullWritable> {
  @Override public void checkInput(HiveInputDescription inputDesc,
      HiveTableSchema schema) {
    Records.verifyType(0, HiveType.INT, schema);
    Records.verifyType(1, HiveType.LIST, schema);
  }

  @Override
  public Iterable<Edge<IntWritable, NullWritable>> getEdges(
      HiveReadableRecord record) {
    return HiveParsing.parseIntNullEdges(record, 1);
  }

  @Override
  public IntWritable getVertexId(HiveReadableRecord record) {
    return HiveParsing.parseIntID(record, 0, getReusableVertexId());
  }

  @Override
  public IntWritable getVertexValue(HiveReadableRecord record) {
    return getReusableVertexValue();
  }
}
