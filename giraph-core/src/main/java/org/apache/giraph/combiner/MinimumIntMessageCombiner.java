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

package org.apache.giraph.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * {@link MessageCombiner} that finds the minimum {@link IntWritable}
 */
public class MinimumIntMessageCombiner
    implements MessageCombiner<WritableComparable, IntWritable> {
  @Override
  public void combine(WritableComparable vertexIndex,
      IntWritable originalMessage, IntWritable messageToCombine) {
    if (originalMessage.get() > messageToCombine.get()) {
      originalMessage.set(messageToCombine.get());
    }
  }

  @Override
  public IntWritable createInitialMessage() {
    return new IntWritable(Integer.MAX_VALUE);
  }
}
