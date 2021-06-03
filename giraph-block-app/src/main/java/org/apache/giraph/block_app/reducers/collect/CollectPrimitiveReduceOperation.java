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
package org.apache.giraph.block_app.reducers.collect;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.reducers.impl.KryoWrappedReduceOperation;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.utils.WritableUtils;

/**
 * Collect primitive values reduce operation
 *
 * @param <S> Primitive Writable type, which has its type ops
 */
public class CollectPrimitiveReduceOperation<S>
    extends KryoWrappedReduceOperation<S, WArrayList<S>> {
  /**
   * Type ops if available, or null
   */
  private PrimitiveTypeOps<S> typeOps;

  /** For reflection only */
  public CollectPrimitiveReduceOperation() {
  }

  public CollectPrimitiveReduceOperation(PrimitiveTypeOps<S> typeOps) {
    this.typeOps = typeOps;
  }

  @Override
  public WArrayList<S> createValue() {
    return createList();
  }

  @Override
  public void reduce(WArrayList<S> reduceInto, S value) {
    reduceInto.addW(value);
  }

  @Override
  public void reduceMerge(final WArrayList<S> reduceInto,
      WArrayList<S> toReduce) {
    ResettableIterator<S> iterator = toReduce.fastIteratorW();
    while (iterator.hasNext()) {
      reduceInto.addW(iterator.next());
    }
  }

  public WArrayList<S> createList() {
    return typeOps.createArrayList();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeClass(typeOps.getTypeClass(), out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    typeOps = TypeOpsUtils.getPrimitiveTypeOps(
        WritableUtils.<S>readClass(in));
  }
}
