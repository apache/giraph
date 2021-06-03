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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests whether ByteValueVertex works -- same test as for DefaultVertex
 * but with different factory method for vertices.
 */
public class TestByteValueVertex extends TestVertexAndEdges {

    protected Vertex<LongWritable, FloatWritable, DoubleWritable>
    instantiateVertex(Class<? extends OutEdges> edgesClass) {
      GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
      giraphConfiguration.setComputationClass(TestComputation.class);
      giraphConfiguration.setOutEdgesClass(edgesClass);
      giraphConfiguration.setVertexClass(ByteValueVertex.class);

      ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration =
                new ImmutableClassesGiraphConfiguration(giraphConfiguration);
      Vertex bv = immutableClassesGiraphConfiguration.createVertex();
      assertTrue(bv instanceof ByteValueVertex);
      return bv;
    }

    @Test
    public void testCachedValue() {
      ByteValueVertex<LongWritable, FloatWritable, DoubleWritable> byteValueVertex =
        (ByteValueVertex<LongWritable, FloatWritable, DoubleWritable>)
          instantiateVertex(ArrayListEdges.class);

      FloatWritable origValue = new FloatWritable(492.2f);
      byteValueVertex.setValue(origValue);

      // Check value is correct
      assertEquals(492.2f, byteValueVertex.getValue().get(), 0.0f);

      // Change value and see it is reflected correctly
      FloatWritable gotValue = byteValueVertex.getValue();
      gotValue.set(33.3f);
      assertEquals(33.3f, byteValueVertex.getValue().get(), 0.0f);

      // Change the object and set that the cached value also changes
      FloatWritable newValue = new FloatWritable(99.9f);
      byteValueVertex.setValue(newValue);
      assertEquals(99.9f, byteValueVertex.getValue().get(), 0.0f);

      // Reference should be now newValue
      assertTrue(newValue == byteValueVertex.getValue());

      // Commit the changes... (called after vertex update)
      byteValueVertex.unwrapMutableEdges();

      // Now the value reference should be new
      assertFalse(newValue == byteValueVertex.getValue());

      // But value data should be correct
      assertEquals(99.9f, byteValueVertex.getValue().get(), 0.0f);
    }
}
