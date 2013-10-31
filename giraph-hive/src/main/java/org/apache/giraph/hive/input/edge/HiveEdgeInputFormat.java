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

package org.apache.giraph.hive.input.edge;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.iterables.EdgeReaderWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.hiveio.input.HiveApiInputFormat;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import java.io.IOException;
import java.util.List;

/**
 * {@link EdgeInputFormat} for reading edges from Hive.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class HiveEdgeInputFormat<I extends WritableComparable,
    E extends Writable> extends EdgeInputFormat<I, E> {
  /** Underlying Hive InputFormat used */
  private final HiveApiInputFormat hiveInputFormat;

  /**
   * Create edge input format.
   */
  public HiveEdgeInputFormat() {
    hiveInputFormat = new HiveApiInputFormat();
  }

  @Override public void checkInputSpecs(Configuration conf) {
    HiveInputDescription inputDesc =
        GiraphHiveConstants.HIVE_VERTEX_INPUT.makeInputDescription(conf);
    HiveTableSchema schema = getTableSchema();
    HiveToEdge<I, E> hiveToEdge = HiveUtils.newHiveToEdge(getConf(), schema);
    hiveToEdge.checkInput(inputDesc, schema);
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
    super.setConf(conf);
    hiveInputFormat.initialize(
        GiraphHiveConstants.HIVE_EDGE_INPUT.makeInputDescription(conf),
        GiraphHiveConstants.HIVE_EDGE_INPUT.getProfileID(conf),
        conf);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    return hiveInputFormat.getSplits(context);
  }

  @Override
  public EdgeReader<I, E> createEdgeReader(InputSplit split,
                                           TaskAttemptContext context)
    throws IOException {

    HiveEdgeReader<I, E> reader = new HiveEdgeReader<I, E>();
    reader.setTableSchema(getTableSchema());

    RecordReader<WritableComparable, HiveReadableRecord> baseReader;
    try {
      baseReader = hiveInputFormat.createRecordReader(split, context);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Could not create edge record reader", e);
    }

    reader.setHiveRecordReader(baseReader);
    return new EdgeReaderWrapper<I, E>(reader);
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
