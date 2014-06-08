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

package org.apache.giraph.hive.input.mapping.examples;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import org.apache.giraph.hive.input.mapping.SimpleHiveToMapping;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Long VertexId, Byte MappingTarget implementation of HiveToMapping
 */
public class LongByteHiveToMapping extends SimpleHiveToMapping<LongWritable,
  ByteWritable> {

  @Override
  public void checkInput(HiveInputDescription inputDesc,
    HiveTableSchema schema) {
    Records.verifyType(0, HiveType.LONG, schema);
    Records.verifyType(1, HiveType.BYTE, schema);
  }

  @Override
  public LongWritable getVertexId(HiveReadableRecord record) {
    LongWritable reusableId = getReusableVertexId();
    reusableId.set(record.getLong(0));
    return reusableId;
  }

  @Override
  public ByteWritable getMappingTarget(HiveReadableRecord record) {
    ByteWritable reusableTarget = getReusableMappingTarget();
    reusableTarget.set(record.getByte(1));
    return reusableTarget;
  }
}
