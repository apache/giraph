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

import java.util.Iterator;

/**
 * Long VertexId, Int Mapping target -> Byte MappingTarget
 * implementation of HiveToMapping
 *
 * The input table has long id, int bucket value
 * we need to translate this to long id & byte bucket value
 */
public class LongInt2ByteHiveToMapping extends SimpleHiveToMapping<LongWritable,
  ByteWritable> {

  /** Number of workers for the job */
  private int numWorkers = 0;

  @Override
  public void initializeRecords(Iterator<HiveReadableRecord> records) {
    super.initializeRecords(records);
    numWorkers = getConf().getMaxWorkers();
    if (numWorkers <= 0 || numWorkers >= 255) {
      throw new IllegalStateException("#workers should be > 0 & < 255");
    }
  }

  @Override
  public void checkInput(HiveInputDescription inputDesc,
    HiveTableSchema schema) {
    Records.verifyType(0, HiveType.LONG, schema);
    Records.verifyType(1, HiveType.INT, schema);
  }

  @Override
  public LongWritable getVertexId(HiveReadableRecord record) {
    long id = record.getLong(0);
    LongWritable reusableId = getReusableVertexId();
    reusableId.set(id);
    return reusableId;
  }

  @Override
  public ByteWritable getMappingTarget(HiveReadableRecord record) {
    int target = record.getInt(1);
    ByteWritable reusableTarget = getReusableMappingTarget();
    int bVal = target % numWorkers;
    if ((bVal >>> 8) != 0) {
      throw new IllegalStateException("target % numWorkers overflows " +
          "byte range");
    }
    reusableTarget.set((byte) bVal);
    return reusableTarget;
  }
}
