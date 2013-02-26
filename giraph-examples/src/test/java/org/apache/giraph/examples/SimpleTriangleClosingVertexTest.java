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

package org.apache.giraph.examples;

import com.google.common.collect.Lists;
import org.apache.giraph.utils.MockUtils;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Contains a simple unit test for {@link SimpleTriangleClosingVertex}
 */
public class SimpleTriangleClosingVertexTest {

  /**
   * Test the behavior of the triangle closing algorithm:
   * does it send all its out edge values to all neighbors?
   */
  @Test
  public void testSuperstepZero() throws Exception {
    // this guy should end up with an array value of 4
    SimpleTriangleClosingVertex vertex =
        new SimpleTriangleClosingVertex();

    SimpleTriangleClosingVertex.IntArrayListWritable alw =
      new SimpleTriangleClosingVertex.IntArrayListWritable();

    MockUtils.MockedEnvironment<IntWritable,
      SimpleTriangleClosingVertex.IntArrayListWritable,
    NullWritable, IntWritable> env =
      MockUtils.prepareVertex(vertex, 0L,
        new IntWritable(1), alw, false);

    vertex.addEdge(EdgeFactory.create(new IntWritable(5)));
    vertex.addEdge(EdgeFactory.create(new IntWritable(7)));

    vertex.compute(Lists.<IntWritable>newArrayList(
      new IntWritable(83), new IntWritable(42)));

    env.verifyMessageSent(new IntWritable(5), new IntWritable(5));
    env.verifyMessageSent(new IntWritable(5), new IntWritable(7));
    env.verifyMessageSent(new IntWritable(7), new IntWritable(5));
    env.verifyMessageSent(new IntWritable(7), new IntWritable(7));
  }

  /** Test behavior of compute() with incoming messages (superstep 1) */
  @Test
  public void testSuperstepOne() throws Exception {
    // see if the vertex interprets its incoming
    // messages properly to verify the algorithm
    SimpleTriangleClosingVertex vertex =
      new SimpleTriangleClosingVertex();

    MockUtils.MockedEnvironment<IntWritable,
      SimpleTriangleClosingVertex.IntArrayListWritable,
      NullWritable, IntWritable>
      env = MockUtils.<IntWritable,
      SimpleTriangleClosingVertex.IntArrayListWritable,
      NullWritable, IntWritable> prepareVertex(
        vertex, 1L, new IntWritable(1), null, false);
      // superstep 1: can the vertex process these correctly?
      vertex.compute(Lists.<IntWritable>newArrayList(
        new IntWritable(7),
        new IntWritable(3),
        new IntWritable(4),
        new IntWritable(7),
        new IntWritable(4),
        new IntWritable(2),
        new IntWritable(4)));
      final String pairCheck = "[4, 7]";
      assertEquals(pairCheck, vertex.getValue().toString());
  }
 }
