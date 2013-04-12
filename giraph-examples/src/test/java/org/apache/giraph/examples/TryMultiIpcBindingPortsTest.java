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

import org.apache.giraph.combiner.MinimumIntCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntIntNullTextInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *  Tests for {@link TryMultiIpcBindingPortsTest}
 */
public class TryMultiIpcBindingPortsTest {

    /**
     * A local integration test on toy data
     */
    @Test
    public void testToyData() throws Exception {

        // a small graph with three components
        String[] graph = new String[] {
                "1 2 3",
                "2 1 4 5",
                "3 1 4",
                "4 2 3 5 13",
                "5 2 4 12 13",
                "12 5 13",
                "13 4 5 12",

                "6 7 8",
                "7 6 10 11",
                "8 6 10",
                "10 7 8 11",
                "11 7 10",

                "9" };

        // run internally
        // fail the first port binding attempt
        GiraphConfiguration conf = new GiraphConfiguration();
        GiraphConstants.FAIL_FIRST_IPC_PORT_BIND_ATTEMPT.set(conf, true);
        conf.setVertexClass(ConnectedComponentsVertex.class);
        conf.setVertexEdgesClass(ByteArrayEdges.class);
        conf.setCombinerClass(MinimumIntCombiner.class);
        conf.setVertexInputFormatClass(IntIntNullTextInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        Iterable<String> results = InternalVertexRunner.run(conf, graph);

        SetMultimap<Integer,Integer> components = parseResults(results);

        Set<Integer> componentIDs = components.keySet();
        assertEquals(3, componentIDs.size());
        assertTrue(componentIDs.contains(1));
        assertTrue(componentIDs.contains(6));
        assertTrue(componentIDs.contains(9));

        Set<Integer> componentOne = components.get(1);
        assertEquals(7, componentOne.size());
        assertTrue(componentOne.contains(1));
        assertTrue(componentOne.contains(2));
        assertTrue(componentOne.contains(3));
        assertTrue(componentOne.contains(4));
        assertTrue(componentOne.contains(5));
        assertTrue(componentOne.contains(12));
        assertTrue(componentOne.contains(13));

        Set<Integer> componentTwo = components.get(6);
        assertEquals(5, componentTwo.size());
        assertTrue(componentTwo.contains(6));
        assertTrue(componentTwo.contains(7));
        assertTrue(componentTwo.contains(8));
        assertTrue(componentTwo.contains(10));
        assertTrue(componentTwo.contains(11));

        Set<Integer> componentThree = components.get(9);
        assertEquals(1, componentThree.size());
        assertTrue(componentThree.contains(9));
    }

    private SetMultimap<Integer,Integer> parseResults(
            Iterable<String> results) {
        SetMultimap<Integer,Integer> components = HashMultimap.create();
        for (String result : results) {
            Iterable<String> parts = Splitter.on('\t').split(result);
            int vertex = Integer.parseInt(Iterables.get(parts, 0));
            int component = Integer.parseInt(Iterables.get(parts, 1));
            components.put(component, vertex);
        }
        return components;
    }
}
