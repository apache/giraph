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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.python.google.common.primitives.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.StreamSupport;

/**
 * Implementation of {@link OutEdges} with int ids and null edge values, backed
 * by the IndexedBitArrayEdges structure proposed in "Panagiotis Liakos, Katia
 * Papakonstantinopoulou, Alex Delis: Realizing Memory-Optimized Distributed
 * Graph Processing. IEEE Trans. Knowl. Data Eng. 30(4): 743-756 (2018)". Note:
 * this implementation is optimized for space usage for graphs exhibiting the
 * locality of reference property, but edge addition and
 * removals are expensive. Parallel edges are not allowed.
 */
public class IndexedBitArrayEdges extends ConfigurableOutEdges<IntWritable, NullWritable>
		implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {

	/** Serialized edges. */
	private byte[] serializedEdges;
	/** Number of edges. */
	private int size;

	@Override
	public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
		initialize();
		int[] sortedEdgesArray = StreamSupport.stream(edges.spliterator(), false).map(Edge::getTargetVertexId)
				.mapToInt(IntWritable::get).sorted().distinct().toArray();
		if (sortedEdgesArray.length == 0) {
			return;
		}
		int currBucket = sortedEdgesArray[0] / Byte.SIZE;
		byte myByte = (byte) 0;
		for (int edge : sortedEdgesArray) {
			int bucket = edge / Byte.SIZE;
			int pos = edge % Byte.SIZE;
			if (bucket == currBucket) {
				myByte = setBit(myByte, pos);
				size += 1;
			} else {
				serializedEdges = addBytes(serializedEdges, toByteArray(currBucket), myByte);
				currBucket = bucket;
				myByte = setBit((byte) 0, pos);
				size += 1;
			}
		}
		serializedEdges = addBytes(serializedEdges, toByteArray(currBucket), myByte);
	}

	@Override
	public void initialize(int capacity) {
		size = 0;
		serializedEdges = new byte[0];
	}

	@Override
	public void initialize() {
		size = 0;
		serializedEdges = new byte[0];
	}

	@Override
	public void add(Edge<IntWritable, NullWritable> edge) {
		int bucket = edge.getTargetVertexId().get() / Byte.SIZE;
		int pos = edge.getTargetVertexId().get() % Byte.SIZE;
		int i = 0;
		boolean done = false;
		while (i < serializedEdges.length) { // if bucket is already there, simply set the appropriate bit
			if (fromByteArray(Arrays.copyOfRange(serializedEdges, i, i + Integer.SIZE / Byte.SIZE)) == bucket) {
				int index = i + Integer.SIZE / Byte.SIZE;
				if (!isSet(serializedEdges[index], pos)) {
					size += 1;
					serializedEdges[index] = setBit(serializedEdges[index], pos);
				}
				done = true;
				break;
			}
			i += Integer.SIZE / Byte.SIZE + 1;
		}
		if (!done) { // we need to add a bucket
			serializedEdges = addBytes(serializedEdges, toByteArray(bucket), setBit((byte) 0, pos));
			size += 1;
		}
	}

	@Override
	public void remove(IntWritable targetVertexId) {
		int bucket = targetVertexId.get() / Byte.SIZE;
		int pos = targetVertexId.get() % Byte.SIZE;
		int i = 0;
		while (i < serializedEdges.length) {
			if (fromByteArray(Arrays.copyOfRange(serializedEdges, i, i + Integer.SIZE / Byte.SIZE)) == bucket) {
				serializedEdges[i + Integer.SIZE / Byte.SIZE] = unsetBit(serializedEdges[i + Integer.SIZE / Byte.SIZE],
						pos);
				--size;
				break;
			}
			i += Integer.SIZE / Byte.SIZE + 1;
		}
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void trim() {
		// Nothing to do
	}
	
	@Override
	public Iterator<Edge<IntWritable, NullWritable>> iterator() {
		if (size == 0) {
			return ImmutableSet.<Edge<IntWritable, NullWritable>>of().iterator();
		} else {
			return new IndexedBitmapEdgeIterator();
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		int serializedEdgesBytesUsed = in.readInt();
		if (serializedEdgesBytesUsed > 0) {
			// Only create a new buffer if the old one isn't big enough
			if (serializedEdges == null || serializedEdgesBytesUsed > serializedEdges.length) {
				serializedEdges = new byte[serializedEdgesBytesUsed];
			}
			in.readFully(serializedEdges, 0, serializedEdgesBytesUsed);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		out.writeInt(serializedEdges.length);
		if (serializedEdges.length > 0) {
			out.write(serializedEdges, 0, serializedEdges.length);
		}
	}

	/** Iterator that reuses the same Edge object. */
	private class IndexedBitmapEdgeIterator extends UnmodifiableIterator<Edge<IntWritable, NullWritable>> {
		/** Representative edge object. */
		private final Edge<IntWritable, NullWritable> representativeEdge = EdgeFactory.create(new IntWritable());
		/** Current edge count */
		private int currentEdge = 0;
		/** Current position */
		private int currentBitPosition = 0;
		private int currentBytePosition = 0;
		/** Index int */
		private int indexInt;
		/** Current byte */
		private Byte myByte;
		/** Current index */
		private byte[] myBytes = new byte[Integer.SIZE / Byte.SIZE];

		@Override
		public boolean hasNext() {
			return currentEdge < size;
		}

		@Override
		public Edge<IntWritable, NullWritable> next() {
			if (currentBitPosition == 8) {
				currentBitPosition = 0;
			}
			if (currentBitPosition == 0) {
				myBytes[0] = serializedEdges[currentBytePosition];
				myBytes[1] = serializedEdges[currentBytePosition + 1];
				myBytes[2] = serializedEdges[currentBytePosition + 2];
				myBytes[3] = serializedEdges[currentBytePosition + 3];
				indexInt = fromByteArray(myBytes);
				myByte = serializedEdges[currentBytePosition + Integer.SIZE / Byte.SIZE];
				currentBytePosition += Integer.SIZE / Byte.SIZE + 1;
			}
			int pos = currentBitPosition;
			int nextPos = 0;
			boolean done = false;
			while (!done) {
				for (int i = pos; i < Byte.SIZE; i++) {
					if (isSet(myByte, i)) {
						done = true;
						nextPos = i;
						currentEdge++;
						currentBitPosition = i + 1;
						break;
					}
				}
				if (!done /* && mapIterator.hasNext() */) {
					myBytes[0] = serializedEdges[currentBytePosition];
					myBytes[1] = serializedEdges[currentBytePosition + 1];
					myBytes[2] = serializedEdges[currentBytePosition + 2];
					myBytes[3] = serializedEdges[currentBytePosition + 3];
					indexInt = fromByteArray(myBytes);
					myByte = serializedEdges[currentBytePosition + Integer.SIZE / Byte.SIZE];
					currentBytePosition += Integer.SIZE / Byte.SIZE + 1;
					pos = 0;
				}
			}
			representativeEdge.getTargetVertexId().set(indexInt * Byte.SIZE + nextPos);
			return representativeEdge;
		}

	}

	/**
	 * tests if bit is set in a byte
	 * 
	 * @param myByte the byte to be tested
	 * @param pos    the position in the byte to be tested
	 * @returns true or false depending on the bit being set
	 * 
	 */
	public static boolean isSet(byte myByte, int pos) {
		return (myByte & (1 << pos)) != 0;
	}

	/**
	 * tests if bit is set in a byte
	 * 
	 * @param byteWritable the byte to be updated
	 * @param pos          the position in the byte to be set
	 * @returns the updated byte
	 * 
	 */
	public static byte setBit(byte myByte, int pos) {
		return (byte) (myByte | (1 << pos));
	}

	/**
	 * tests if bit is set in a byte
	 * 
	 * @param my_byte the byte to be updated
	 * @param pos     the position in the byte to be unset
	 * @returns the updated byte
	 * 
	 */
	public static byte unsetBit(byte myByte, int pos) {
		return (byte) (myByte & ~(1 << pos));
	}

	/**
	 * converts an integer to a 4-byte length array holding
	 * its binary representation
	 * 
	 * @param value the integer to be converted
	 * @return the 4-byte length byte array holding the integer's
	 * binary representation
	 */
	public static byte[] toByteArray(int value) {
		return new byte[] { (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value };
	}

	/**
	 * converts a 4-byte length byte array to its respective integer
	 * 
	 * @param bytes a 4-byte length byte array
	 * @return the integer represented in the byte array
	 */
	public static int fromByteArray(byte[] bytes) {
		return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
	}

	/**
	 * returns a byte arrray with the concatenation of the three input parameters
	 * 
	 * @param original a byte array
	 * @param bindex a byte array
	 * @param myByte a byte
	 * @return a byte arrray with the concatenation of the three input parameters
	 * */
	public static byte[] addBytes(byte[] original, byte[] bindex, byte myByte) {
		byte[] destination = new byte[original.length + Integer.SIZE / Byte.SIZE + 1];
		System.arraycopy(original, 0, destination, 0, original.length);
		System.arraycopy(bindex, 0, destination, original.length, Integer.SIZE / Byte.SIZE);
		destination[destination.length - 1] = myByte;
		return destination;
	}

}
