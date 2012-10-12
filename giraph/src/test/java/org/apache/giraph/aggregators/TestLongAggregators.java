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
package org.apache.giraph.aggregators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class TestLongAggregators {

  @Test
  public void testMaxAggregator() {
    LongMaxAggregator max = new LongMaxAggregator();
    max.aggregate(new LongWritable(2L));
    max.aggregate(new LongWritable(3L));
    assertEquals(3L, max.getAggregatedValue().get());
    max.setAggregatedValue(new LongWritable(1L));
    assertEquals(1L, max.getAggregatedValue().get());
    LongWritable lw = max.createInitialValue();
    assertNotNull(lw);
  }

  @Test
  public void testMinAggregator() {
    LongMinAggregator min = new LongMinAggregator();
    min.aggregate(new LongWritable(3L));
    min.aggregate(new LongWritable(2L));
    assertEquals(2L, min.getAggregatedValue().get());
    min.setAggregatedValue(new LongWritable(3L));
    assertEquals(3L, min.getAggregatedValue().get());
    LongWritable lw = min.createInitialValue();
    assertNotNull(lw);
  }

  @Test
  public void testOverwriteAggregator() {
    LongOverwriteAggregator overwrite = new LongOverwriteAggregator();
    overwrite.aggregate(new LongWritable(1L));
    assertEquals(1L, overwrite.getAggregatedValue().get());
    overwrite.aggregate(new LongWritable(2L));
    assertEquals(2L, overwrite.getAggregatedValue().get());
    overwrite.setAggregatedValue(new LongWritable(3L));
    assertEquals(3L, overwrite.getAggregatedValue().get());
    LongWritable lw = overwrite.createInitialValue();
    assertNotNull(lw);
  }
  
  @Test
  public void testProductAggregator() {
    LongProductAggregator product = new LongProductAggregator();
    product.aggregate(new LongWritable(6L));
    product.aggregate(new LongWritable(7L));
    assertEquals(42L, product.getAggregatedValue().get());
    product.setAggregatedValue(new LongWritable(1L));
    assertEquals(1L, product.getAggregatedValue().get());
    LongWritable lw = product.createInitialValue();
    assertNotNull(lw);
  }

  @Test
  public void testSumAggregator() {
    LongSumAggregator sum = new LongSumAggregator();
    sum.aggregate(new LongWritable(1L));
    sum.aggregate(new LongWritable(2L));
    assertEquals(3L, sum.getAggregatedValue().get());
    sum.setAggregatedValue(new LongWritable(4L));
    assertEquals(4L, sum.getAggregatedValue().get());
    LongWritable lw = sum.createInitialValue();
    assertNotNull(lw);
  }

}
