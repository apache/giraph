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
package org.apache.giraph.types.ops;

import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.BasicInt2ObjectOpenHashMap;
import org.apache.giraph.types.ops.collections.BasicArrayList;
import org.apache.giraph.types.ops.collections.BasicArrayList.BasicIntArrayList;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.types.ops.collections.BasicSet.BasicIntOpenHashSet;
import org.apache.hadoop.io.IntWritable;

/** TypeOps implementation for working with IntWritable type */
public enum IntTypeOps implements PrimitiveIdTypeOps<IntWritable> {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<IntWritable> getTypeClass() {
    return IntWritable.class;
  }

  @Override
  public IntWritable create() {
    return new IntWritable();
  }

  @Override
  public IntWritable createCopy(IntWritable from) {
    return new IntWritable(from.get());
  }

  @Override
  public void set(IntWritable to, IntWritable from) {
    to.set(from.get());
  }

  @Override
  public BasicSet<IntWritable> createOpenHashSet(int capacity) {
    return new BasicIntOpenHashSet(capacity);
  }

  @Override
  public BasicArrayList<IntWritable> createArrayList(int capacity) {
    return new BasicIntArrayList(capacity);
  }

  @Override
  public <V> Basic2ObjectMap<IntWritable, V> create2ObjectOpenHashMap(
      int capacity) {
    return new BasicInt2ObjectOpenHashMap<>(capacity);
  }
}
