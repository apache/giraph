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

import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

public class TestBooleanAggregators {

  @Test
  public void testAndAggregator() {
    BooleanAndAggregator and = new BooleanAndAggregator();
    assertEquals(true, and.getAggregatedValue().get());
    and.aggregate(new BooleanWritable(true));
    assertEquals(true, and.getAggregatedValue().get());
    and.aggregate(new BooleanWritable(false));
    assertEquals(false, and.getAggregatedValue().get());
    and.setAggregatedValue(new BooleanWritable(true));
    assertEquals(true, and.getAggregatedValue().get());
    BooleanWritable bw = and.createInitialValue();
    assertNotNull(bw);
  }

  @Test
  public void testOrAggregator() {
    BooleanOrAggregator or = new BooleanOrAggregator();
    assertEquals(false, or.getAggregatedValue().get());
    or.aggregate(new BooleanWritable(false));
    assertEquals(false, or.getAggregatedValue().get());
    or.aggregate(new BooleanWritable(true));
    assertEquals(true, or.getAggregatedValue().get());
    or.setAggregatedValue(new BooleanWritable(false));
    assertEquals(false, or.getAggregatedValue().get());
    BooleanWritable bw = or.createInitialValue();
    assertNotNull(bw);
  }

  @Test
  public void testOverwriteAggregator() {
    BooleanOverwriteAggregator overwrite = new BooleanOverwriteAggregator();
    overwrite.aggregate(new BooleanWritable(true));
    assertEquals(true, overwrite.getAggregatedValue().get());
    overwrite.aggregate(new BooleanWritable(false));
    assertEquals(false, overwrite.getAggregatedValue().get());
    overwrite.setAggregatedValue(new BooleanWritable(true));
    assertEquals(true, overwrite.getAggregatedValue().get());
    BooleanWritable bw = overwrite.createInitialValue();
    assertNotNull(bw);
  }

}
