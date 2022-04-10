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
package org.apache.giraph.types.ops.collections.set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.types.ops.${type.camel}TypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.utils.Varint;
import org.apache.hadoop.io.${type.camel}Writable;

import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}Iterator;
<#if type.hasBigFastutil()>
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}OpenHashBigSet;
</#if>
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}OpenHashSet;
import it.unimi.dsi.fastutil.${type.lower}s.${type.camel}Set;

${generated_message}

/** ${type.camel}Writable implementation of BasicSet */
public final class Basic${type.camel}OpenHashSet
  implements BasicSet<${type.camel}Writable> {
  /** Set */
  private final ${type.camel}Set set;

  /** Constructor */
  public Basic${type.camel}OpenHashSet() {
    set = new ${type.camel}OpenHashSet();
  }

  /**
   * Constructor
   *
   * @param capacity Capacity
   */
<#if type.hasBigFastutil()>
  public Basic${type.camel}OpenHashSet(long capacity) {
    if (capacity <= MAX_OPEN_HASHSET_CAPACITY) {
      set = new ${type.camel}OpenHashSet((int) capacity);
    } else {
      set = new ${type.camel}OpenHashBigSet(capacity);
    }
  }
<#else>
  public Basic${type.camel}OpenHashSet(int capacity) {
    set = new ${type.camel}OpenHashSet(capacity);
  }
</#if>  

  @Override
  public void clear() {
    set.clear();
  }

  @Override
  public long size() {
<#if type.hasBigFastutil()>
    if (set instanceof ${type.camel}OpenHashBigSet) {
      return ((${type.camel}OpenHashBigSet) set).size64();
    }
</#if>  
    return set.size();
  }

  @Override
  public void trim(long n) {
<#if type.hasBigFastutil()>
    if (set instanceof ${type.camel}OpenHashSet) {
      ((${type.camel}OpenHashSet) set).trim((int) Math.max(set.size(), n));
    } else {
      ((${type.camel}OpenHashBigSet) set).trim(Math.max(set.size(), n));
    }
<#else>
    ((${type.camel}OpenHashSet) set).trim((int) Math.max(set.size(), n));
</#if>  
  }

  @Override
  public boolean add(${type.camel}Writable value) {
    return set.add(value.get());
  }

  @Override
  public boolean contains(${type.camel}Writable value) {
    return set.contains(value.get());
  }

  @Override
  public PrimitiveIdTypeOps<${type.camel}Writable> getElementTypeOps() {
    return ${type.camel}TypeOps.INSTANCE;
  }

  @Override
  public void write(DataOutput out) throws IOException {
<#if type.hasBigFastutil()>
    Varint.writeUnsignedVarLong(size(), out);
<#else>
    Varint.writeUnsignedVarInt(set.size(), out);
</#if>  
    ${type.camel}Iterator iter = set.iterator();
    while (iter.hasNext()) {
      out.write${type.camel}(iter.next${type.camel}());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
<#if type.hasBigFastutil()>
    long size = Varint.readUnsignedVarLong(in);
<#else>
    int size = Varint.readUnsignedVarInt(in);
</#if>  
    set.clear();
    trim(size);
<#if type.hasBigFastutil()>
    for (long i = 0; i < size; ++i) {
<#else>
    for (int i = 0; i < size; ++i) {
</#if>  
      set.add(in.read${type.camel}());
    }
  }
}
