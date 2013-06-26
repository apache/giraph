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
package org.apache.giraph.hive.input.edge.examples;

import org.apache.giraph.hive.common.HiveParsing;
import org.apache.giraph.hive.input.edge.SimpleHiveToEdge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Preconditions;

/**
 * A simple HiveToEdge with integer IDs, no edge value, that assumes the Hive
 * table is made up of [source,target] columns.
 */
public class HiveIntNullEdge
    extends SimpleHiveToEdge<IntWritable, NullWritable> {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(HiveIntNullEdge.class);

  @Override public void checkInput(HiveInputDescription inputDesc,
      HiveTableSchema schema) {
    Preconditions.checkArgument(schema.columnType(0) == HiveType.INT);
    Preconditions.checkArgument(schema.columnType(1) == HiveType.INT);
  }

  @Override public NullWritable getEdgeValue(HiveReadableRecord hiveRecord) {
    return NullWritable.get();
  }

  @Override
  public IntWritable getSourceVertexId(HiveReadableRecord hiveRecord) {
    return HiveParsing.parseIntID(hiveRecord, 0);
  }

  @Override
  public IntWritable getTargetVertexId(HiveReadableRecord hiveRecord) {
    return HiveParsing.parseIntID(hiveRecord, 1);
  }
}
