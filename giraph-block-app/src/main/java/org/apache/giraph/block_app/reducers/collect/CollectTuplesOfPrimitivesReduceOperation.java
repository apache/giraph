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
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.reducers.impl.KryoWrappedReduceOperation;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.giraph.utils.WritableUtils;

/**
 * Collect tuples of primitive values reduce operation
 */
public class CollectTuplesOfPrimitivesReduceOperation
    extends KryoWrappedReduceOperation<List<Object>, List<WArrayList>> {
  /**
   * Type ops if available, or null
   */
  private List<PrimitiveTypeOps> typeOpsList;

  /** For reflection only */
  public CollectTuplesOfPrimitivesReduceOperation() {
  }

  public CollectTuplesOfPrimitivesReduceOperation(
      List<PrimitiveTypeOps> typeOpsList) {
    this.typeOpsList = typeOpsList;
  }

  @Override
  public List<WArrayList> createValue() {
    List<WArrayList> ret = new ArrayList<>(typeOpsList.size());
    for (PrimitiveTypeOps typeOps : typeOpsList) {
      ret.add(typeOps.createArrayList());
    }
    return ret;
  }

  @Override
  public void reduce(List<WArrayList> reduceInto, List<Object> value) {
    for (int i = 0; i < reduceInto.size(); i++) {
      reduceInto.get(i).addW(value.get(i));
    }
  }

  @Override
  public void reduceMerge(final List<WArrayList> reduceInto,
      List<WArrayList> toReduce) {
    for (int i = 0; i < reduceInto.size(); i++) {
      ResettableIterator iterator = toReduce.get(i).fastIteratorW();
      while (iterator.hasNext()) {
        reduceInto.get(i).addW(iterator.next());
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(typeOpsList.size());
    for (PrimitiveTypeOps typeOps : typeOpsList) {
      WritableUtils.writeClass(typeOps.getTypeClass(), out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    typeOpsList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      typeOpsList.add(TypeOpsUtils.getPrimitiveTypeOps(
          WritableUtils.readClass(in)));
    }
  }
}
