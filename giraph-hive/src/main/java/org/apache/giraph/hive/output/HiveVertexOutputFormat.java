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

package org.apache.giraph.hive.output;

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.hiveio.output.HiveApiOutputFormat;
import com.facebook.hiveio.record.HiveWritableRecord;

import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_DATABASE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_PARTITION;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_PROFILE_ID;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_TABLE;

/**
 * VertexOutputFormat using Hive
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class HiveVertexOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends VertexOutputFormat<I, V, E> {
  /** Underlying Hive OutputFormat used */
  private final HiveApiOutputFormat hiveOutputFormat;

  /**
   * Create vertex output format
   */
  public HiveVertexOutputFormat() {
    hiveOutputFormat = new HiveApiOutputFormat();
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E, Writable> conf) {
    super.setConf(conf);
    HiveUtils.initializeHiveOutput(
        hiveOutputFormat,
        HIVE_VERTEX_OUTPUT_PROFILE_ID.get(conf),
        HIVE_VERTEX_OUTPUT_DATABASE.get(conf),
        HIVE_VERTEX_OUTPUT_TABLE.get(conf),
        HIVE_VERTEX_OUTPUT_PARTITION.get(conf),
        conf);
  }

  @Override
  public VertexWriter<I, V, E> createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    RecordWriter<WritableComparable, HiveWritableRecord> baseWriter =
        hiveOutputFormat.getRecordWriter(context);
    HiveVertexWriter<I, V, E> writer = new HiveVertexWriter<I, V, E>();
    writer.setBaseWriter(baseWriter);
    writer.setTableSchema(hiveOutputFormat.getTableSchema(getConf()));
    return writer;
  }

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    hiveOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return hiveOutputFormat.getOutputCommitter(context);
  }
}
