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

package org.apache.giraph.vertex;

import com.google.common.collect.ImmutableList;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A vertex with no value, edges, or messages. Just an ID, nothing more.
 */
public abstract class IntNullNullNullVertex extends Vertex<IntWritable,
    NullWritable, NullWritable, NullWritable> {
  @Override
  public void setEdges(Iterable<Edge<IntWritable, NullWritable>> edges) { }

  @Override
  public Iterable<Edge<IntWritable, NullWritable>> getEdges() {
    return ImmutableList.of();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    getId().write(out);
    out.writeBoolean(isHalted());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int id = in.readInt();
    initialize(new IntWritable(id), NullWritable.get());
    boolean halt = in.readBoolean();
    if (halt) {
      voteToHalt();
    } else {
      wakeUp();
    }
  }
}
