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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.mockito.Mockito;

import java.util.Map;

/** contains a simple unit test for {@link SimpleShortestPathsVertex} */
public class SimpleShortestPathVertexTest extends TestCase {

    /** test the behavior when a shorter path to a vertex has been found */
    public void testOnShorterPathFound() throws Exception {

        SimpleShortestPathsVertex vertex = new SimpleShortestPathsVertex();
        vertex.addEdge(new LongWritable(10L), new FloatWritable(2.5f));
        vertex.addEdge(new LongWritable(20L), new FloatWritable(0.5f));

        MockUtils.MockedEnvironment<LongWritable, DoubleWritable, FloatWritable,
                DoubleWritable> env = MockUtils.prepareVertex(vertex, 1L,
                new LongWritable(7L), new DoubleWritable(Double.MAX_VALUE),
                false);

        Mockito.when(env.getConfiguration().getLong(
                SimpleShortestPathsVertex.SOURCE_ID,
                SimpleShortestPathsVertex.SOURCE_ID_DEFAULT)).thenReturn(2L);

        vertex.compute(Lists.newArrayList(new DoubleWritable(2),
                new DoubleWritable(1.5)).iterator());

        assertTrue(vertex.isHalted());
        assertEquals(1.5, vertex.getVertexValue().get());

        env.verifyMessageSent(new LongWritable(10L), new DoubleWritable(4));
        env.verifyMessageSent(new LongWritable(20L), new DoubleWritable(2));
    }

    /** test the behavior when a new, but not shorter path to a vertex has been found */
    public void testOnNoShorterPathFound() throws Exception {

        SimpleShortestPathsVertex vertex = new SimpleShortestPathsVertex();
        vertex.addEdge(new LongWritable(10L), new FloatWritable(2.5f));
        vertex.addEdge(new LongWritable(20L), new FloatWritable(0.5f));

        MockUtils.MockedEnvironment<LongWritable, DoubleWritable, FloatWritable,
                DoubleWritable> env = MockUtils.prepareVertex(vertex, 1L,
                new LongWritable(7L), new DoubleWritable(0.5), false);

        Mockito.when(env.getConfiguration().getLong(
                SimpleShortestPathsVertex.SOURCE_ID,
                SimpleShortestPathsVertex.SOURCE_ID_DEFAULT)).thenReturn(2L);

        vertex.compute(Lists.newArrayList(new DoubleWritable(2),
                new DoubleWritable(1.5)).iterator());

        assertTrue(vertex.isHalted());
        assertEquals(0.5, vertex.getVertexValue().get());

        env.verifyNoMessageSent();
    }

    /** a local integration test on toy data */
    public void testToyData() throws Exception {

        /* a small four vertex graph */
        String[] graph = new String[] {
                "[1,0,[[2,1],[3,3]]]",
                "[2,0,[[3,1],[4,10]]]",
                "[3,0,[[4,2]]]",
                "[4,0,[]]" };

        /* start from vertex 1 */
        Map<String,String> params = Maps.newHashMap();
        params.put(SimpleShortestPathsVertex.SOURCE_ID, "1");

        /* run internally */
        Iterable<String> results = InternalVertexRunner.run(
                SimpleShortestPathsVertex.class,
                SimpleShortestPathsVertex.
                        SimpleShortestPathsVertexInputFormat.class,
                SimpleShortestPathsVertex.
                        SimpleShortestPathsVertexOutputFormat.class,
                params, graph);

        Map<Long, Double> distances = parseDistances(results);

        /* verify results */
        assertNotNull(distances);
        assertEquals(4, distances.size());
        assertEquals(0.0, distances.get(1L));
        assertEquals(1.0, distances.get(2L));
        assertEquals(2.0, distances.get(3L));
        assertEquals(4.0, distances.get(4L));
    }

    private Map<Long,Double> parseDistances(Iterable<String> results) {
        Map<Long,Double> distances =
                Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                JSONArray jsonVertex = new JSONArray(line);
                distances.put(jsonVertex.getLong(0), jsonVertex.getDouble(1));
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "Couldn't get vertex from line " + line, e);
            }
        }
        return distances;
    }
}
