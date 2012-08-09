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

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TestIntAggregators {

  @Test
  public void testMaxAggregator() {
    IntMaxAggregator max = new IntMaxAggregator();
    max.aggregate(new IntWritable(2));
    max.aggregate(new IntWritable(3));
    assertEquals(3, max.getAggregatedValue().get());
    max.setAggregatedValue(new IntWritable(1));
    assertEquals(1, max.getAggregatedValue().get());
    IntWritable iw = max.createInitialValue();
    assertNotNull(iw);
  }

  @Test
  public void testMinAggregator() {
    IntMinAggregator min = new IntMinAggregator();
    min.aggregate(new IntWritable(3));
    min.aggregate(new IntWritable(2));
    assertEquals(2, min.getAggregatedValue().get());
    min.setAggregatedValue(new IntWritable(3));
    assertEquals(3, min.getAggregatedValue().get());
    IntWritable iw = min.createInitialValue();
    assertNotNull(iw);
  }

  @Test
  public void testOverwriteAggregator() {
    IntOverwriteAggregator overwrite = new IntOverwriteAggregator();
    overwrite.aggregate(new IntWritable(1));
    assertEquals(1, overwrite.getAggregatedValue().get());
    overwrite.aggregate(new IntWritable(2));
    assertEquals(2, overwrite.getAggregatedValue().get());
    overwrite.setAggregatedValue(new IntWritable(3));
    assertEquals(3, overwrite.getAggregatedValue().get());
    IntWritable iw = overwrite.createInitialValue();
    assertNotNull(iw);
  }
  
  @Test
  public void testProductAggregator() {
    IntProductAggregator product = new IntProductAggregator();
    product.aggregate(new IntWritable(6));
    product.aggregate(new IntWritable(7));
    assertEquals(42, product.getAggregatedValue().get());
    product.setAggregatedValue(new IntWritable(1));
    assertEquals(1, product.getAggregatedValue().get());
    IntWritable iw = product.createInitialValue();
    assertNotNull(iw);
  }

  @Test
  public void testSumAggregator() {
    IntSumAggregator sum = new IntSumAggregator();
    sum.aggregate(new IntWritable(1));
    sum.aggregate(new IntWritable(2));
    assertEquals(3, sum.getAggregatedValue().get());
    sum.setAggregatedValue(new IntWritable(4));
    assertEquals(4, sum.getAggregatedValue().get());
    IntWritable iw = sum.createInitialValue();
    assertNotNull(iw);
  }

}
