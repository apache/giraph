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

import com.google.common.collect.Lists;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexedBitArrayEdgesTest {
	private static Edge<IntWritable, NullWritable> createEdge(int id) {
		return EdgeFactory.create(new IntWritable(id));
	}

	private static void assertEdges(IndexedBitArrayEdges edges, int... expected) {
		int index = 0;
		for (Edge<IntWritable, NullWritable> edge : (Iterable<Edge<IntWritable, NullWritable>>) edges) {
			Assert.assertEquals(expected[index], edge.getTargetVertexId().get());
			index++;
		}
		Assert.assertEquals(expected.length, index);
	}

	private IndexedBitArrayEdges getEdges() {
		GiraphConfiguration gc = new GiraphConfiguration();
		GiraphConstants.VERTEX_ID_CLASS.set(gc, IntWritable.class);
		GiraphConstants.EDGE_VALUE_CLASS.set(gc, NullWritable.class);
		ImmutableClassesGiraphConfiguration<IntWritable, Writable, NullWritable> conf = new ImmutableClassesGiraphConfiguration<IntWritable, Writable, NullWritable>(
				gc);
		IndexedBitArrayEdges ret = new IndexedBitArrayEdges();
		ret.setConf(new ImmutableClassesGiraphConfiguration<IntWritable, Writable, NullWritable>(conf));
		return ret;
	}

	@Test
	public void testEdges() {
		IndexedBitArrayEdges edges = getEdges();

		List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayList(createEdge(1), createEdge(2),
				createEdge(4));

		edges.initialize(initialEdges);
		assertEdges(edges, 1, 2, 4);

		edges.add(EdgeFactory.createReusable(new IntWritable(3)));
		assertEdges(edges, 1, 2, 3, 4); // order matters, it's an array

		edges.remove(new IntWritable(2));
		assertEdges(edges, 1, 3, 4);
	}

	@Test
	public void testInitialize() {
		IndexedBitArrayEdges edges = getEdges();

		List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayList(createEdge(1), createEdge(2),
				createEdge(4));

		edges.initialize(initialEdges);
		assertEdges(edges, 1, 2, 4);

		edges.add(EdgeFactory.createReusable(new IntWritable(3)));
		assertEdges(edges, 1, 2, 3, 4);

		edges.initialize();
		assertEquals(0, edges.size());
	}

	@Test
	public void testInitializeUnsorted() {
		IndexedBitArrayEdges edges = getEdges();

		List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayList(createEdge(3), createEdge(4),
				createEdge(1));

		edges.initialize(initialEdges);
		assertEdges(edges, 1, 3, 4);

		edges.add(EdgeFactory.createReusable(new IntWritable(2)));
		
		assertEdges(edges, 1, 2, 3, 4);

		edges.initialize();
		assertEquals(0, edges.size());
	}

	@Test
	public void testMutateEdges() {
		IndexedBitArrayEdges edges = getEdges();

		edges.initialize();

		// Add 10 edges with id i, for i = 0..9
		for (int i = 0; i < 10; ++i) {
			edges.add(createEdge(i));
		}

		// Remove edges with even id
		for (int i = 0; i < 10; i += 2) {
			edges.remove(new IntWritable(i));
		}

		// We should now have 5 edges
		assertEquals(5, edges.size());
		// The edge ids should be all odd
		for (Edge<IntWritable, NullWritable> edge : (Iterable<Edge<IntWritable, NullWritable>>) edges) {
			assertEquals(1, edge.getTargetVertexId().get() % 2);
		}
	}

	@Test
	public void testSerialization() throws IOException {
		IndexedBitArrayEdges edges = getEdges();

		edges.initialize();

		// Add 10 edges with id i, for i = 0..9
		for (int i = 0; i < 10; ++i) {
			edges.add(createEdge(i));
		}

		// Remove edges with even id
		for (int i = 0; i < 10; i += 2) {
			edges.remove(new IntWritable(i));
		}

		// We should now have 5 edges
		assertEdges(edges, 1, 3, 5, 7, 9); // id order matter because of the implementation

		ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
		DataOutputStream tempBuffer = new DataOutputStream(arrayStream);

		edges.write(tempBuffer);
		tempBuffer.close();

		byte[] binary = arrayStream.toByteArray();

		assertTrue("Serialized version should not be empty ", binary.length > 0);

		edges = getEdges();
		edges.readFields(new DataInputStream(new ByteArrayInputStream(binary)));

		assertEquals(5, edges.size());

		int[] ids = new int[] { 1, 3, 5, 7, 9 };
		int index = 0;

		for (Edge<IntWritable, NullWritable> edge : (Iterable<Edge<IntWritable, NullWritable>>) edges) {
			assertEquals(ids[index], edge.getTargetVertexId().get());
			index++;
		}
		assertEquals(ids.length, index);
	}

	@Test
	public void testParallelEdges() {
		IndexedBitArrayEdges edges = getEdges();

		List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayList(createEdge(2), createEdge(2),
				createEdge(2));

		edges.initialize(initialEdges);
		
		assertEquals(1, edges.size());
	}

	@Test
	public void testEdgeValues() {
		IndexedBitArrayEdges edges = getEdges();
		Set<Integer> testValues = new HashSet<Integer>();
		testValues.add(0);
		testValues.add(Integer.MAX_VALUE / 2);
		testValues.add(Integer.MAX_VALUE);

		List<Edge<IntWritable, NullWritable>> initialEdges = new ArrayList<Edge<IntWritable, NullWritable>>();
		for (Integer id : testValues) {
			initialEdges.add(createEdge(id));
		}

		edges.initialize(initialEdges);

		Iterator<Edge<IntWritable, NullWritable>> edgeIt = edges.iterator();
		while (edgeIt.hasNext()) {
			int value = edgeIt.next().getTargetVertexId().get();
			assertTrue("Unknown edge found " + value, testValues.remove(value));
		}
	}

	@Test
	public void testAddedSmallerValues() {
		IndexedBitArrayEdges edges = getEdges();

		List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayList(createEdge(100));

		edges.initialize(initialEdges);

		for (int i = 0; i < 16; i++) {
			edges.add(createEdge(i));
		}

		assertEquals(17, edges.size());
	}

	@Test
	public void testCompressedSize() throws IOException {
		IndexedBitArrayEdges edges = getEdges();

		edges.initialize();
		
		// Add 10 edges with id i, for i = 0..9
		// to create on large interval
		for (int i = 0; i < 10; ++i) {
			edges.add(createEdge(i));
		}
		
		// Add a residual edge
		edges.add(createEdge(23));
		
		assertEquals(11, edges.size());
		
		DataOutput dataOutput = Mockito.mock(DataOutput.class);
		edges.write(dataOutput);

		// size of serializedEdges byte array should be equal to 15:
		// 5 (bucket 0: 0-7) + 5 (bucket 1: 8-10) + 5 (bucket 2: 23)
		ArgumentCaptor<byte[]> serializedEdgesCaptop = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(dataOutput).write(serializedEdgesCaptop.capture(), Mockito.anyInt(), Mockito.anyInt());
		assertEquals(15, serializedEdgesCaptop.getValue().length);
	}
}
