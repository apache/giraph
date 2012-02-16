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

package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Used by {@link BspOutputFormat} since some versions of Hadoop
 * require that a RecordWriter is returned from getRecordWriter.
 * Does nothing, except insures that write is never called.
 */
public class BspRecordWriter extends RecordWriter<Text, Text> {

  @Override
  public void close(TaskAttemptContext context)
    throws IOException, InterruptedException {
    // Do nothing
  }

  @Override
  public void write(Text key, Text value)
    throws IOException, InterruptedException {
    throw new IOException("write: Cannot write with " +
        getClass().getName() +
        ".  Should never be called");
  }
}
