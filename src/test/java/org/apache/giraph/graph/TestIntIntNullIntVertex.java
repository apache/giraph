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

package org.apache.giraph.graph;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link IntIntNullIntVertex}.
 */
public class TestIntIntNullIntVertex {
  /**
   * Simple instantiable class that extends {@link IntIntNullIntVertex}.
   */
  private static class MyIntIntNullVertex extends IntIntNullIntVertex {
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {
    }
  }

  @Test
  public void testSerialize() throws IOException {
    IntIntNullIntVertex vertex = new MyIntIntNullVertex();

    Map<IntWritable, NullWritable> edges =
        new HashMap<IntWritable, NullWritable>();
    edges.put(new IntWritable(3), NullWritable.get());
    edges.put(new IntWritable(47), NullWritable.get());

    List<IntWritable> messages = Lists.newArrayList(new IntWritable(943),
        new IntWritable(571));

    vertex.initialize(new IntWritable(23), new IntWritable(7), edges,
        messages);
    vertex.voteToHalt();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);
    vertex.write(out);

    IntIntNullIntVertex vertex1 = new MyIntIntNullVertex();

    ByteArrayInputStream inStream = new ByteArrayInputStream(
        outStream.toByteArray());
    DataInput in = new DataInputStream(inStream);
    vertex1.readFields(in);

    assertEquals(2, vertex1.getNumEdges());
    assertEquals(Lists.newArrayList(new IntWritable(943),
        new IntWritable(571)), Lists.newArrayList(vertex1.getMessages()));
    assertEquals(true, vertex1.isHalted());
  }
}
