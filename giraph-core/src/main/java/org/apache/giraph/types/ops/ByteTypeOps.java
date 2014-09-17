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

import org.apache.giraph.types.ops.collections.BasicArrayList.BasicByteArrayList;
import org.apache.hadoop.io.ByteWritable;

/** TypeOps implementation for working with ByteWritable type */
public enum ByteTypeOps implements PrimitiveTypeOps<ByteWritable> {
  /** Singleton instance */
  INSTANCE();

  @Override
  public Class<ByteWritable> getTypeClass() {
    return ByteWritable.class;
  }

  @Override
  public ByteWritable create() {
    return new ByteWritable();
  }

  @Override
  public ByteWritable createCopy(ByteWritable from) {
    return new ByteWritable(from.get());
  }

  @Override
  public void set(ByteWritable to, ByteWritable from) {
    to.set(from.get());
  }

  @Override
  public BasicByteArrayList createArrayList(int capacity) {
    return new BasicByteArrayList(capacity);
  }
}
