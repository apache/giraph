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
package org.apache.giraph.block_app.reducers;

import org.apache.giraph.reducers.impl.KryoWrappedReduceOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Extracts top N largest elements
 *
 * @param <S> Single value type, objects passed on workers
 */
public class TopNReduce<S extends Comparable<S>>
  extends KryoWrappedReduceOperation<S, PriorityQueue<S>> {
  private int capacity;

  public TopNReduce(int capacity) {
    this.capacity = capacity;
  }

  public TopNReduce() { }

  @Override
  public PriorityQueue<S> createValue() {
    return new PriorityQueue<S>();
  }

  @Override
  public void reduce(PriorityQueue<S> heap, S value) {
    if (capacity == 0) {
      return;
    }

    if (heap.size() < capacity) {
      heap.add(value);
    } else {
      S head = heap.peek();
      if (head.compareTo(value) < 0) {
        heap.poll();
        heap.add(value);
      }
    }
  }

  @Override
  public void reduceMerge(
    PriorityQueue<S> reduceInto,
    PriorityQueue<S> toReduce
  ) {
    for (S element : toReduce) {
      reduce(reduceInto, element);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(capacity);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    capacity = in.readInt();
  }
}
