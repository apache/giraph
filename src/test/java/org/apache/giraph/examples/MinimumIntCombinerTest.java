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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class MinimumIntCombinerTest {

    @Test
    public void testCombiner() throws Exception {

        VertexCombiner<IntWritable, IntWritable> combiner =
                new MinimumIntCombiner();

        Iterable<IntWritable> result = combiner.combine(
                new IntWritable(1), Arrays.asList(
                new IntWritable(39947466), new IntWritable(199),
                new IntWritable(19998888), new IntWritable(42)));
        assertTrue(result.iterator().hasNext());
        assertEquals(42, result.iterator().next().get());
    }
}
