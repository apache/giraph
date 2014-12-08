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
package org.apache.giraph.debugger.examples.randomwalk;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * DebugConfig for checking message constraint in random walk implementation.
 */
public class RandomWalkMessageConstraintDebugConfig
  extends
  DebugConfig<LongWritable, IntWritable, NullWritable, IntWritable,
  IntWritable> {

  @Override
  public boolean shouldCatchExceptions() {
    return false;
  }

  @Override
  public boolean shouldDebugVertex(
    Vertex<LongWritable, IntWritable, NullWritable> vertex, long superstepNo) {
    return false;
  }

  @Override
  public boolean shouldCheckMessageIntegrity() {
    return true;
  }

  @Override
  public boolean isMessageCorrect(LongWritable srcId, LongWritable dstId,
    IntWritable message, long superstepNo) {
    return message.get() > 0;
  }

}
