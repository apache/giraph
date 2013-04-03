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

package org.apache.giraph.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Vertex input format that only allows setting vertex id and value. It can
 * be used in conjunction with {@link EdgeInputFormat}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 */
public abstract class VertexValueInputFormat<I extends WritableComparable,
    V extends Writable> extends VertexInputFormat<I, V, Writable> {
  /**
   * Create a {@link VertexValueReader} for a given split. The framework will
   * call {@link VertexValueReader#initialize(InputSplit,
   * TaskAttemptContext)} before the split is used.
   *
   * @param split The split to be read
   * @param context The information about the task
   * @return A new vertex value reader
   * @throws IOException
   */
  public abstract VertexValueReader<I, V> createVertexValueReader(
      InputSplit split, TaskAttemptContext context) throws IOException;

  @Override
  public final VertexReader<I, V, Writable> createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return createVertexValueReader(split, context);
  }
}
