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

<#if type.id>
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.Basic${type.camel}2ObjectOpenHashMap;
import org.apache.giraph.types.ops.collections.BasicSet.Basic${type.camel}OpenHashSet;
</#if>
import org.apache.giraph.types.ops.collections.array.W${type.camel}ArrayList;
<#if type.id>
import org.apache.giraph.types.ops.collections.WritableWriter;
</#if>
import org.apache.hadoop.io.${type.camel}Writable;

import java.io.DataInput;
import java.io.IOException;

${generated_message}
<#if type.numeric && type.id>
  <#assign parent_type = "PrimitiveIdTypeOps<${type.camel}Writable>, NumericTypeOps<${type.camel}Writable>"> 
<#elseif type.numeric>
  <#assign parent_type = "PrimitiveTypeOps<${type.camel}Writable>, NumericTypeOps<${type.camel}Writable>">
<#elseif type.id>
  <#assign parent_type = "PrimitiveIdTypeOps<${type.camel}Writable>">
<#else>
  <#assign parent_type = "PrimitiveTypeOps<${type.camel}Writable>">
</#if>
<#macro cast_if_needed_v expr><#if type.lower == "byte">(${type.lower}) ${expr}<#else>${expr}</#if></#macro>
<#macro cast_if_needed_e expr><#if type.lower == "byte">(${type.lower}) (${expr})<#else>${expr}</#if></#macro>

/** TypeOps implementation for working with ${type.camel}Writable type */
public enum ${type.camel}TypeOps implements
    ${parent_type} {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<${type.camel}Writable> getTypeClass() {
    return ${type.camel}Writable.class;
  }

  @Override
  public ${type.camel}Writable create() {
    return new ${type.camel}Writable();
  }

  @Override
  public ${type.camel}Writable createCopy(${type.camel}Writable from) {
    return new ${type.camel}Writable(from.get());
  }

  @Override
  public void set(${type.camel}Writable to, ${type.camel}Writable from) {
    to.set(from.get());
  }

  @Override
  public W${type.camel}ArrayList createArrayList() {
    return new W${type.camel}ArrayList();
  }

  @Override
  public W${type.camel}ArrayList createArrayList(int capacity) {
    return new W${type.camel}ArrayList(capacity);
  }

  @Override
  public W${type.camel}ArrayList readNewArrayList(DataInput in) throws IOException {
    return W${type.camel}ArrayList.readNew(in);
  }
<#if type.id>

  @Override
  public Basic${type.camel}OpenHashSet createOpenHashSet() {
    return new Basic${type.camel}OpenHashSet();
  }

  @Override
  public Basic${type.camel}OpenHashSet createOpenHashSet(long capacity) {
    return new Basic${type.camel}OpenHashSet(capacity);
  }

  @Override
  public <V> Basic${type.camel}2ObjectOpenHashMap<V> create2ObjectOpenHashMap(
      WritableWriter<V> valueWriter) {
    return new Basic${type.camel}2ObjectOpenHashMap<>(valueWriter);
  }

  @Override
  public <V> Basic${type.camel}2ObjectOpenHashMap<V> create2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter) {
    return new Basic${type.camel}2ObjectOpenHashMap<>(capacity, valueWriter);
  }
</#if>
<#if type.numeric>

  @Override
  public ${type.camel}Writable createZero() {
    return new ${type.camel}Writable(<@cast_if_needed_v expr="0"/>);
  }

  @Override
  public ${type.camel}Writable createOne() {
    return new ${type.camel}Writable(<@cast_if_needed_v expr="1"/>);
  }

  @Override
  public ${type.camel}Writable createMinNegativeValue() {
    return new ${type.camel}Writable(${type.boxed}.${type.floating?string("NEGATIVE_INFINITY", "MIN_VALUE")});
  }

  @Override
  public ${type.camel}Writable createMaxPositiveValue() {
    return new ${type.camel}Writable(${type.boxed}.${type.floating?string("POSITIVE_INFINITY", "MAX_VALUE")});
  }

  @Override
  public void plusInto(${type.camel}Writable value, ${type.camel}Writable increment) {
    value.set(<@cast_if_needed_e expr="value.get() + increment.get()"/>);
  }

  @Override
  public void multiplyInto(${type.camel}Writable value, ${type.camel}Writable multiplier) {
    value.set(<@cast_if_needed_e expr="value.get() * multiplier.get()"/>);
  }

  @Override
  public void negate(${type.camel}Writable value) {
    value.set(<@cast_if_needed_e expr="-value.get()"/>);
  }

  @Override
  public int compare(${type.camel}Writable value1, ${type.camel}Writable value2) {
    return ${type.boxed}.compare(value1.get(), value2.get());
  }
</#if>
}
