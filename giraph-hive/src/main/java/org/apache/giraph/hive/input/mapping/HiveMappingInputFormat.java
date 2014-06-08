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

import com.facebook.hiveio.input.HiveApiInputFormat;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.MappingReader;
import org.apache.giraph.io.iterables.MappingReaderWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

import static org.apache.giraph.hive.common.HiveUtils.newHiveToMapping;

/**
 * HiveMappingInputFormat extends MappingInputFormat
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 * @param <B> mappingTarget type
 */
public class HiveMappingInputFormat<I extends WritableComparable,
  V extends Writable, E extends Writable, B extends Writable>
  extends MappingInputFormat<I, V, E, B> {
  /** Underlying Hive InputFormat used */
  private final HiveApiInputFormat hiveInputFormat;

  /**
   * Create vertex input format
   */
  public HiveMappingInputFormat() {
    hiveInputFormat = new HiveApiInputFormat();
  }

  @Override
  public void checkInputSpecs(Configuration conf) {
    HiveInputDescription inputDesc =
        GiraphHiveConstants.HIVE_MAPPING_INPUT.makeInputDescription(conf);
    HiveTableSchema schema = getTableSchema();
    HiveToMapping<I, B> hiveToMapping = newHiveToMapping(getConf(), schema);
    hiveToMapping.checkInput(inputDesc, schema);
  }


  @Override
  public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    super.setConf(conf);
    hiveInputFormat.initialize(
        GiraphHiveConstants.HIVE_MAPPING_INPUT.makeInputDescription(conf),
        GiraphHiveConstants.HIVE_MAPPING_INPUT.getProfileID(conf),
        conf);
  }


  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    return hiveInputFormat.getSplits(context);
  }

  @Override
  public MappingReader<I, V, E, B> createMappingReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    HiveMappingReader<I, B> reader = new HiveMappingReader<>();
    reader.setTableSchema(getTableSchema());

    RecordReader<WritableComparable, HiveReadableRecord> baseReader;
    try {
      baseReader = hiveInputFormat.createRecordReader(split, context);
    } catch (InterruptedException e) {
      throw new IOException("Could not create map reader", e);
    }

    reader.setHiveRecordReader(baseReader);
    return new MappingReaderWrapper<>(reader);
  }


  /**
   * Get Hive table schema
   *
   * @return Hive table schema
   */
  private HiveTableSchema getTableSchema() {
    return hiveInputFormat.getTableSchema(getConf());
  }
}
