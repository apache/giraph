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

package org.apache.giraph.hive.input.mapping;

import com.facebook.hiveio.record.HiveReadableRecord;
import org.apache.giraph.hive.common.DefaultConfigurableAndTableSchemaAware;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.hive.input.RecordReaderWrapper;
import org.apache.giraph.io.iterables.GiraphReader;
import org.apache.giraph.mapping.MappingEntry;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * MappingReader using Hive
 *
 * @param <I> vertexId type
 * @param <B> mappingTarget type
 */
public class HiveMappingReader<I extends WritableComparable,
  B extends Writable>
  extends DefaultConfigurableAndTableSchemaAware<I, Writable, Writable>
  implements GiraphReader<MappingEntry<I, B>> {
  /** Underlying Hive RecordReader used */
  private RecordReader<WritableComparable, HiveReadableRecord> hiveRecordReader;
  /** Hive To Mapping */
  private HiveToMapping<I, B> hiveToMapping;

  /**
   * Get hiverecord reader
   *
   * @return hiveRecordReader
   */
  public RecordReader<WritableComparable, HiveReadableRecord>
  getHiveRecordReader() {
    return hiveRecordReader;
  }

  public void setHiveRecordReader(
      RecordReader<WritableComparable, HiveReadableRecord> hiveRecordReader) {
    this.hiveRecordReader = hiveRecordReader;
  }

  @Override
  public void initialize(InputSplit inputSplit,
    TaskAttemptContext context) throws IOException, InterruptedException {
    hiveRecordReader.initialize(inputSplit, context);
    hiveToMapping = HiveUtils.newHiveToMapping(getConf(), getTableSchema());
    hiveToMapping.initializeRecords(
        new RecordReaderWrapper<>(hiveRecordReader));
  }

  @Override
  public void close() throws IOException {
    hiveRecordReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return hiveRecordReader.getProgress();
  }

  @Override
  public boolean hasNext() {
    return hiveToMapping.hasNext();
  }


  @Override
  public MappingEntry<I, B> next() {
    return hiveToMapping.next();
  }

  @Override
  public void remove() {
    hiveToMapping.remove();
  }

}
