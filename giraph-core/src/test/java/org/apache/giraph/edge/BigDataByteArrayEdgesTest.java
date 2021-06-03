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
package org.apache.giraph.edge;

import junit.framework.TestCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class BigDataByteArrayEdgesTest
        extends TestCase
{
    private static ImmutableClassesGiraphConfiguration createConfiguration() {
        return createConfiguration(LongWritable.class);
    }
    private static <T extends Writable> ImmutableClassesGiraphConfiguration createConfiguration(Class<T> clazz) {
        GiraphConfiguration conf = new GiraphConfiguration();
        GiraphConstants.VERTEX_ID_CLASS.set(conf, LongWritable.class);
        GiraphConstants.EDGE_VALUE_CLASS.set(conf, clazz);
        return new ImmutableClassesGiraphConfiguration<>(conf);
    }

    private static BigDataByteArrayEdges<LongWritable, LongWritable> createBigDataByteArrayEdges(int maxBufferSize) {
        BigDataByteArrayEdges<LongWritable, LongWritable> edges = new BigDataByteArrayEdges<>(maxBufferSize);
        edges.setConf(createConfiguration());
        return edges;
    }

    private static List<Edge<LongWritable, LongWritable>> createEdges(Edge<LongWritable, LongWritable> ...edges) {
        return new ArrayList<>(Arrays.asList(edges));
    }

    private static ExtendedDataOutput createDataOutput(List<Edge<LongWritable, LongWritable>> edges) throws IOException {
        final int NUM_FLUFF_INTS = 8;
        ExtendedDataOutput dataOutput = createConfiguration().createExtendedDataOutput();
        dataOutput.writeInt(1); // 1 data output in big data set
        dataOutput.writeInt(0); // byte count (to be filled later after edges have been written)

        edges.forEach(e ->  {
            try {
                WritableUtils.writeEdge(dataOutput, e);
            }
            catch (IOException exc) {

            }
        });
        dataOutput.writeInt(Integer.BYTES, dataOutput.getPos() - 2*Integer.BYTES); //update byte count with actual number
        dataOutput.writeInt(edges.size());
        for (int i = 0; i < NUM_FLUFF_INTS; i++) {
            dataOutput.write(i); //some fluff at the end - make sure BigDataByteArrayEdges can handle it
        }
        return dataOutput;
    }
    private static ExtendedDataInput createDataInput(ExtendedDataOutput output) {
        return new ImmutableClassesGiraphConfiguration<>(createConfiguration())
                .createExtendedDataInput(output.toByteArray());
    }
    private static ExtendedDataInput createDataInput(List<Edge<LongWritable, LongWritable>> edges) throws IOException {
        return createDataInput(createDataOutput(edges));
    }

    private static Edge<LongWritable, LongWritable> edge(long vertex, long value) {
        DefaultEdge edge = new DefaultEdge<>();
        edge.setTargetVertexId(new LongWritable(vertex));
        edge.setValue(new LongWritable(value));
        return edge;
    }

    private static Edge<LongWritable, Text> edge(long vertex, String value) {
        DefaultEdge edge = new DefaultEdge<>();
        edge.setTargetVertexId(new LongWritable(vertex));
        edge.setValue(new Text(value));
        return edge;
    }

    private static <T extends Writable> void assertEdges(List<Edge<LongWritable, T>> expectedList,
            BigDataByteArrayEdges<LongWritable, T> edges) {
        assertEquals(expectedList.size(), edges.size());
        Iterator<Edge<LongWritable, T>> expected = expectedList.iterator();
        Iterator<Edge<LongWritable, T>> actual = edges.iterator();

        while (actual.hasNext() && expected.hasNext()) {
            Edge<LongWritable, T> expectedValue = expected.next();
            Edge<LongWritable, T> actualValue = actual.next();
            assertEquals(expectedValue.getTargetVertexId(), actualValue.getTargetVertexId());
            assertEquals(expectedValue.getValue(), actualValue.getValue());
        }
        assertTrue("Not equal size", !actual.hasNext() && !expected.hasNext());
    }

    //region test initialization
    @Test
    public void testInitializeEmpty()
    {
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(8);
        edges.initialize();
        assertEdges(Arrays.asList(), edges);
    }

    @Test
    public void testInitializeInitialCapacity()
    {
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(64);
        edges.initialize(32);
        assertEdges(Arrays.asList(), edges);
    }

    @Test
    public void testInitializeWithEdges()
    {
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(10);
        List<Edge<LongWritable, LongWritable>> initEdges =
                createEdges(edge(1, 4), edge(2, 5), edge(3, 6));
        edges.initialize(initEdges);
        assertEdges(initEdges, edges);
    }

    @Test
    public void testInitializeMultipleTimes()
    {
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(10);
        List<Edge<LongWritable, LongWritable>> initEdges =
                createEdges(edge(1, 4), edge(2, 5), edge(3, 6));
        edges.initialize(initEdges);
        assertEdges(initEdges, edges);
        edges.initialize();
        assertEdges(Arrays.asList(), edges); //empty after reinitialization
        initEdges.forEach(e -> edges.add(e));
        assertEdges(initEdges, edges);
    }
    //endregion

    //region add/remove edges
    @Test
    public void testAddEdgeThatExceedsMaxBufferSize()
    {
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(3);
        edges.initialize(3);
        List<Edge<LongWritable, LongWritable>> inputEdges = createEdges(edge(1, 3), edge(2, 4));
        inputEdges.forEach(e -> edges.add(e));
        assertEdges(inputEdges, edges);
    }

    @Test
    public void testAddEdgesTwoPerBuffer()
    {
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                //each edge is 16 bytes
                createBigDataByteArrayEdges(32);
        edges.initialize();
        List<Edge<LongWritable, LongWritable>> inputEdges = new ArrayList<>();
        int vertex;
        for (vertex = 0; vertex < 49; vertex++) { // make sure last data output is not full
            inputEdges.add(edge(vertex, vertex % 3));
        }
        inputEdges.forEach(e -> edges.add(e));
        assertEdges(inputEdges, edges);
        edges.trim(); //verify that when last data output is trimmed, current is update correctly
        assertEdges(inputEdges, edges);
        inputEdges.add(edge(vertex, vertex % 3));
        edges.add(inputEdges.get(inputEdges.size() - 1));
        assertEdges(inputEdges, edges);
    }

    @Test
    public void testEdgesOfDifferentSize()
    {
        BigDataByteArrayEdges<LongWritable, Text> edges = new BigDataByteArrayEdges<>(32);
        edges.setConf(createConfiguration(Text.class));
        edges.initialize();
        List<Edge<LongWritable, Text>> inputEdges = new ArrayList<>();
        inputEdges.add(edge(1, "Short String"));
        inputEdges.add(edge(1, "Very Very Long, Super Long String"));
        inputEdges.forEach(e -> edges.add(e));
        assertEdges(inputEdges, edges);
    }

    @Test
    public void testEdgesOfDefaultMaxSize()
    {
        BigDataByteArrayEdges<LongWritable, Text> edges = new BigDataByteArrayEdges<>();
        edges.setConf(createConfiguration(Text.class));
        edges.initialize();
        List<Edge<LongWritable, Text>> inputEdges = new ArrayList<>();
        for (long vertex = 0; vertex < 1000; vertex++) {
            inputEdges.add(edge(vertex, String.format("VertexValue%d", vertex % 3)));
        }
        inputEdges.forEach(e -> edges.add(e));
        assertEdges(inputEdges, edges);
    }

    @Test
    public void testAddEdgesTwoPerBufferWithUnusedBytes()
    {
        //each edge is 16 bytes, 8 bytes remaining at the end of each buffer will be trimmed
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(40);
        edges.initialize(40);
        List<Edge<LongWritable, LongWritable>> inputEdges = new ArrayList<>();
        for (long vertex = 0; vertex < 50; vertex++) {
            inputEdges.add(edge(vertex, vertex % 3));
        }
        inputEdges.forEach(e -> edges.add(e));
        assertEdges(inputEdges, edges);
        edges.trim();
        assertEdges(inputEdges, edges);
    }

    @Test
    public void testRemoveEdgesIncludesCurrentBuffer()
    {
        //each edge is 16 bytes, 8 bytes remaining at the end of each buffer will be trimmed
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(40);
        edges.initialize(40);
        List<Edge<LongWritable, LongWritable>> inputEdges =
                createEdges(edge(1, 1002), edge(2, 1003), // remove(2) - remove 1 edge, remove(1) - remove 1 edge
                        edge(3, 1004), edge(4, 1005), // remove(2) -  0 edges from buffer 2
                        edge(2, 1006), edge(2, 1007));// remove(2) -  all edges from buffer 3,
        // current buffer should revert to previous one

        List<Edge<LongWritable, LongWritable>> expectedEdges = inputEdges.stream().filter(e -> e.getTargetVertexId().get() != 2)
                .collect(Collectors.toList());
        inputEdges.forEach(e -> edges.add(e));
        assertEdges(inputEdges, edges);
        edges.remove(new LongWritable(2));
        assertEdges(expectedEdges, edges);

        inputEdges.add(edge(6, 1008));
        expectedEdges.add(inputEdges.get(inputEdges.size() - 1));
        edges.add(inputEdges.get(inputEdges.size() - 1)); //confirm that add works after removal

        assertEdges(expectedEdges, edges);

        edges.remove(new LongWritable(5)); //remove item that doesn't exist
        assertEdges(expectedEdges, edges);
        edges.remove(new LongWritable(1));
        expectedEdges = expectedEdges.stream().filter(e -> e.getTargetVertexId().get() != 1)
                .collect(Collectors.toList());
        assertEdges(expectedEdges, edges);
        inputEdges.add(edge(7, 1009));
        expectedEdges.add(inputEdges.get(inputEdges.size() - 1));
        edges.add(inputEdges.get(inputEdges.size() - 1)); //confirm that add works after removal
        assertEdges(expectedEdges, edges);
        inputEdges.forEach(e -> edges.remove(e.getTargetVertexId())); // remove everything
        assertEdges(Arrays.asList(), edges);  //confirm that edges is empty

        //verify that after removing all elements edges is in a state that add works correctly
        inputEdges.forEach(e -> edges.add(e));
        assertEdges(inputEdges, edges);
    }
    //endregion

    //region read/write
    @Test
    public void testReadWriteExtendedDataInputOutputBuffers() throws IOException
    {
        // input is smaller than max, readFields will not deserialize edges
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(40);
        edges.initialize(40);
        List<Edge<LongWritable, LongWritable>> inputEdges = createEdges(edge(1, 2), edge(3, 4));
        ExtendedDataInput dataInput = createDataInput(inputEdges);
        edges.readFields(dataInput);
        assertEdges(inputEdges, edges);
        ExtendedDataOutput output = createConfiguration().createExtendedDataOutput();
        edges.write(output);

        // input is larger than max, readFields will deserialize edges and split them across multiple buffers
        edges = createBigDataByteArrayEdges(16);
        edges.initialize(16);
        edges.readFields(createDataInput(output));
        assertEdges(inputEdges, edges);

        //input is larger than max, readFields will deserialize edges and split them across multiple buffers.
        //make sure fluff at the end is ignored
        dataInput = createDataInput(inputEdges);
        edges.readFields(dataInput);
        assertEdges(inputEdges, edges);
    }

    @Test
    public void testReadWriteBigDataInputOutputBuffers() throws IOException
    {
        // input is smaller than max, readFields will not deserialize edges
        BigDataByteArrayEdges<LongWritable, LongWritable> edges =
                createBigDataByteArrayEdges(40);
        edges.initialize(40);
        List<Edge<LongWritable, LongWritable>> inputEdges = createEdges(edge(1, 2), edge(3, 4));
        DataInput dataInput = createDataInput(inputEdges);
        edges.readFields(dataInput);
        assertEdges(inputEdges, edges);
        ExtendedDataOutput output = createConfiguration().createExtendedDataOutput();
        edges.write(output);

        // input is larger than max, readFields will deserialize edges and split them across multiple buffers
        edges = createBigDataByteArrayEdges(16);
        edges.initialize(16);
        edges.readFields(createDataInput(output));
        assertEdges(inputEdges, edges);
    }
    //endregion
}
