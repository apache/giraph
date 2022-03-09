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
package org.apache.giraph.debugger.examples.graphcoloring;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * DebugConfig for checking message constraints of graph coloring.
 */
public class GraphColoringMessageConstraintDebugConfig
  extends
  DebugConfig<LongWritable, VertexValue, NullWritable, Message, Message> {

  @Override
  public boolean shouldCheckMessageIntegrity() {
    return true;
  }

  @Override
  public boolean isMessageCorrect(LongWritable srcId, LongWritable dstId,
    Message message, long superstepNo) {
    // TODO check message type validity based on phase
    // TODO check message type validity based on sender and receiver's state
    return message.getType() != null && srcId.get() != dstId.get();
  }

}
