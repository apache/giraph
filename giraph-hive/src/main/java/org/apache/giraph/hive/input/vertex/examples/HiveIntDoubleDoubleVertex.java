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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.hive.common.HiveParsing;
import org.apache.giraph.hive.input.vertex.SimpleHiveToVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * Simple HiveToVertex that reads vertices with integer IDs, Double vertex
 * values, and edges with Double values.
 */
public class HiveIntDoubleDoubleVertex extends SimpleHiveToVertex<IntWritable,
    DoubleWritable, DoubleWritable> {
  @Override public void checkInput(HiveInputDescription inputDesc,
      HiveTableSchema schema) {
    Records.verifyType(0, HiveType.INT, schema);
    Records.verifyType(1, HiveType.DOUBLE, schema);
    Records.verifyType(2, HiveType.MAP, schema);
  }

  @Override public Iterable<Edge<IntWritable, DoubleWritable>> getEdges(
      HiveReadableRecord record) {
    return HiveParsing.parseIntDoubleEdges(record, 2);
  }

  @Override
  public IntWritable getVertexId(HiveReadableRecord record) {
    return HiveParsing.parseIntID(record, 0, getReusableVertexId());
  }

  @Override
  public DoubleWritable getVertexValue(HiveReadableRecord record) {
    return HiveParsing.parseDoubleWritable(record, 1, getReusableVertexValue());
  }
}
