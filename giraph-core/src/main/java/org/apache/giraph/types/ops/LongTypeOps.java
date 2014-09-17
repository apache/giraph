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
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.BasicLong2ObjectOpenHashMap;
import org.apache.giraph.types.ops.collections.BasicArrayList;
import org.apache.giraph.types.ops.collections.BasicArrayList.BasicLongArrayList;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.types.ops.collections.BasicSet.BasicLongOpenHashSet;
import org.apache.hadoop.io.LongWritable;

/** TypeOps implementation for working with LongWritable type */
public enum LongTypeOps implements PrimitiveIdTypeOps<LongWritable> {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<LongWritable> getTypeClass() {
    return LongWritable.class;
  }

  @Override
  public LongWritable create() {
    return new LongWritable();
  }

  @Override
  public LongWritable createCopy(LongWritable from) {
    return new LongWritable(from.get());
  }

  @Override
  public void set(LongWritable to, LongWritable from) {
    to.set(from.get());
  }

  @Override
  public BasicSet<LongWritable> createOpenHashSet(int capacity) {
    return new BasicLongOpenHashSet(capacity);
  }

  @Override
  public BasicArrayList<LongWritable> createArrayList(int capacity) {
    return new BasicLongArrayList(capacity);
  }

  @Override
  public <V> Basic2ObjectMap<LongWritable, V> create2ObjectOpenHashMap(
      int capacity) {
    return new BasicLong2ObjectOpenHashMap<>(capacity);
  }
}
