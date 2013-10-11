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
package org.apache.giraph.hive.common;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Helpers for parsing with Hive.
 */
public class HiveParsing {
  /** Don't construct */
  private HiveParsing() { }

  /**
   * Parse a byte from a Hive record
   * @param record Hive record to parse
   * @param columnIndex offset of column in row
   * @return byte
   */
  public static byte parseByte(HiveReadableRecord record, int columnIndex) {
    return record.getByte(columnIndex);
  }

  /**
   * Parse a int from a Hive record
   * @param record Hive record to parse
   * @param columnIndex offset of column in row
   * @return int
   */
  public static int parseInt(HiveReadableRecord record, int columnIndex) {
    return record.getInt(columnIndex);
  }

  /**
   * Parse a Integer ID from a Hive record
   * @param record Hive record to parse
   * @param columnIndex offset of column in row
   * @param reusableId Reusable vertex id object
   * @return IntWritable ID
   */
  public static IntWritable parseIntID(HiveReadableRecord record,
      int columnIndex, IntWritable reusableId) {
    reusableId.set(parseInt(record, columnIndex));
    return reusableId;
  }

  /**
   * Parse a Long ID from a Hive record
   * @param record Hive record to parse
   * @param columnIndex offset of column in row
   * @param reusableId Reusable vertex id object
   * @return LongWritable ID
   */
  public static LongWritable parseLongID(HiveReadableRecord record,
      int columnIndex, LongWritable reusableId) {
    reusableId.set(record.getLong(columnIndex));
    return reusableId;
  }

  /**
   * Parse a weight from a Hive record
   * @param record Hive record to parse
   * @param columnIndex offset of column in row
   * @param reusableDoubleWritable Reusable DoubleWritable object
   *
   * @return DoubleWritable weight
   */
  public static DoubleWritable parseDoubleWritable(HiveReadableRecord record,
      int columnIndex, DoubleWritable reusableDoubleWritable) {
    reusableDoubleWritable.set(record.getDouble(columnIndex));
    return reusableDoubleWritable;
  }

  /**
   * Parse edges as mappings of integer => double (id to weight)
   * @param record Hive record to parse
   * @param columnIndex offset of column in row
   * @return edges
   */
  @SuppressWarnings("unchecked")
  public static Iterable<Edge<IntWritable, DoubleWritable>> parseIntDoubleEdges(
      HiveReadableRecord record, int columnIndex) {
    Object edgesObj = record.get(columnIndex);
    if (edgesObj == null) {
      return ImmutableList.of();
    }
    Map<Long, Double> readEdges = (Map<Long, Double>) edgesObj;
    List<Edge<IntWritable, DoubleWritable>> edges =
        Lists.newArrayListWithCapacity(readEdges.size());
    for (Map.Entry<Long, Double> entry : readEdges.entrySet()) {
      edges.add(EdgeFactory.create(new IntWritable(entry.getKey().intValue()),
          new DoubleWritable(entry.getValue())));
    }
    return edges;
  }

  /**
   * Parse edges from a list
   * @param record hive record
   * @param index column index
   * @return iterable of edges
   */
  public static Iterable<Edge<IntWritable, NullWritable>> parseIntNullEdges(
      HiveReadableRecord record, int index) {
    List<Long> ids = (List<Long>) record.get(index);
    if (ids == null) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<Edge<IntWritable, NullWritable>> builder =
        ImmutableList.builder();
    for (long id : ids) {
      builder.add(EdgeFactory.create(new IntWritable((int) id)));
    }
    return builder.build();
  }

  /**
   * Parse edges as mappings of long => double (id to weight)
   * @param record Hive record to parse
   * @param columnIndex offset of column in row
   * @return edges
   */
  @SuppressWarnings("unchecked")
  public static Iterable<Edge<LongWritable, DoubleWritable>>
  parseLongDoubleEdges(HiveReadableRecord record, int columnIndex) {
    Object edgesObj = record.get(columnIndex);
    if (edgesObj == null) {
      return ImmutableList.of();
    }
    Map<Long, Double> readEdges = (Map<Long, Double>) edgesObj;
    List<Edge<LongWritable, DoubleWritable>> edges =
        Lists.newArrayListWithCapacity(readEdges.size());
    for (Map.Entry<Long, Double> entry : readEdges.entrySet()) {
      edges.add(EdgeFactory.create(new LongWritable(entry.getKey()),
          new DoubleWritable(entry.getValue())));
    }
    return edges;
  }
}
