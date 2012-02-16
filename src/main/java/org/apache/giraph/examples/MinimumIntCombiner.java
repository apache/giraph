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

package org.apache.giraph.examples;

import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link VertexCombiner} that finds the minimum {@link IntWritable}
 */
public class MinimumIntCombiner
    extends VertexCombiner<IntWritable, IntWritable> {
  @Override
  public Iterable<IntWritable> combine(IntWritable target,
      Iterable<IntWritable> messages) throws IOException {
    int minimum = Integer.MAX_VALUE;
    for (IntWritable message : messages) {
      if (message.get() < minimum) {
        minimum = message.get();
      }
    }
    List<IntWritable> value = new ArrayList<IntWritable>();
    value.add(new IntWritable(minimum));

    return value;
  }
}
