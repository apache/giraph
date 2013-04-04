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

import com.facebook.giraph.hive.input.HiveApiInputFormat;
import com.facebook.giraph.hive.record.HiveReadableRecord;
import java.io.IOException;
import java.util.List;
import org.apache.giraph.hive.common.HiveProfiles;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.iterables.EdgeReaderWrapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
    hiveInputFormat.setMyProfileId(HiveProfiles.EDGE_INPUT_PROFILE_ID);
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
    reader.setTableSchema(hiveInputFormat.getTableSchema(getConf()));

    RecordReader<WritableComparable, HiveReadableRecord> baseReader;
    try {
      baseReader = hiveInputFormat.createRecordReader(split, context);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Could not create edge record reader", e);
    }

    reader.setHiveRecordReader(baseReader);
    return new EdgeReaderWrapper<I, E>(reader);
  }
}
