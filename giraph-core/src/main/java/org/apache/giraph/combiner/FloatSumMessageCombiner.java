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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A combiner that sums float-valued messages
 *
 * Use SumMessageCombiner.DOUBLE instead.
 */
@Deprecated
public class FloatSumMessageCombiner
    implements MessageCombiner<WritableComparable, FloatWritable> {
  @Override
  public void combine(WritableComparable vertexIndex,
      FloatWritable originalMessage, FloatWritable messageToCombine) {
    originalMessage.set(originalMessage.get() + messageToCombine.get());
  }

  @Override
  public FloatWritable createInitialMessage() {
    return new FloatWritable(0);
  }
}
