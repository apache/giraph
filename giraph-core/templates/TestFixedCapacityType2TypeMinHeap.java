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

package org.apache.giraph.types.heaps;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

${generated_message}
<#macro cast2><#if type2.lower == "byte">(${type2.lower}) </#if></#macro>
<#macro precision2><#if type2.lower == "float" || type2.lower == "double">, 0</#if></#macro>

public class TestFixedCapacity${type1.camel}${type2.camel}MinHeap {
  @Test
  public void testHeap() {
    int heapCapacity = 5;
    FixedCapacity${type1.camel}${type2.camel}MinHeap heap =
        new FixedCapacity${type1.camel}${type2.camel}MinHeap(heapCapacity);
    ${type1.lower}[] keys = new ${type1.lower}[]{0, 1, 0, 10, 20, 0, 3, 4};
    ${type2.lower}[] values = new ${type2.lower}[]{
        <@cast2/>0, <@cast2/>1, <@cast2/>2, <@cast2/>2, <@cast2/>2,
        <@cast2/>3, <@cast2/>3, <@cast2/>4};

    List<Integer> positions = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      positions.add(i);
    }
    Collections.shuffle(positions);
    for (Integer position : positions) {
      heap.add(keys[position], values[position]);
    }

    for (int i = keys.length - heapCapacity; i < keys.length; i++) {
      Assert.assertEquals(heap.size(), heapCapacity);
      Assert.assertEquals(heap.getMinKey(), keys[i]);
      Assert.assertEquals(heap.getMinValue(), values[i]<@precision2/>);
      heap.removeMin();
      heapCapacity--;
    }
    Assert.assertTrue(heap.isEmpty());
  }
}
