package org.apache.giraph.edge;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.weakref.jmx.com.google.common.collect.Iterators;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;

/**
 * Implementation of {@link OutEdges} with int ids and null edge values, backed
 * by the IntervalResidualEdges structure proposed in "Panagiotis Liakos, Katia
 * Papakonstantinopoulou, Alex Delis: Realizing Memory-Optimized Distributed
 * Graph Processing. IEEE Trans. Knowl. Data Eng. 30(4): 743-756 (2018)". Note:
 * this implementation is optimized for space usage, but edge addition and
 * removals are expensive. Parallel edges are not allowed.
 */
public class IntervalResidualEdges extends ConfigurableOutEdges<IntWritable, NullWritable>
		implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {

	/** Minimum interval length is equal to 2 */
	private static final int MIN_INTERVAL_LENGTH = 2;
	
	/** Maximum interval length is equal to 254 */
	private static final int MAX_INTERVAL_LENGTH = 254;
	
	/** Serialized Intervals and Residuals */
	private byte[] intervalsAndResiduals;

	/** Number of edges stored in compressed array */
	private int size;

	@Override
	public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
		compress(StreamSupport.stream(edges.spliterator(), false).map(Edge::getTargetVertexId).mapToInt(IntWritable::get).toArray());
	}
	
	@Override
	public void initialize(int capacity) {
		size = 0;
		intervalsAndResiduals = null;
	}

	@Override
	public void initialize() {
		size = 0;
		intervalsAndResiduals = null;
	}

	@Override
	public void add(Edge<IntWritable, NullWritable> edge) {
	    // Note that this is very expensive (decompresses all edges and recompresses them again).
		compress(StreamSupport.stream(((Iterable<Edge<IntWritable, NullWritable>>)() -> Iterators.concat(this.iterator(), ImmutableSet.of(edge).iterator())).spliterator(), false).map(Edge::getTargetVertexId).mapToInt(IntWritable::get).sorted().toArray());
	}

	@Override
	public void remove(IntWritable targetVertexId) {
	    // Note that this is very expensive (decompresses all edges and recompresses them again).
		initialize(Iterables.filter(this, edge -> !((Edge<IntWritable, NullWritable>) edge).getTargetVertexId().equals(targetVertexId)));
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public Iterator<Edge<IntWritable, NullWritable>> iterator() {
		if (size == 0) {
			return ImmutableSet.<Edge<IntWritable, NullWritable>>of().iterator();
		} else {
			return new IntervalResidualEdgeIterator();
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		int intervalResidualEdgesBytesUsed = in.readInt();
		if (intervalResidualEdgesBytesUsed > 0) {
			// Only create a new buffer if the old one isn't big enough
			if (intervalsAndResiduals == null || intervalResidualEdgesBytesUsed > intervalsAndResiduals.length) {
				intervalsAndResiduals = new byte[intervalResidualEdgesBytesUsed];
			}
			in.readFully(intervalsAndResiduals, 0, intervalResidualEdgesBytesUsed);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		out.writeInt(intervalsAndResiduals.length);
		if (intervalsAndResiduals.length > 0) {
			out.write(intervalsAndResiduals, 0, intervalsAndResiduals.length);
		}
	}

	@Override
	public void trim() {
		/* Nothing to do */
	}
	
	/** Receives an integer array of successors and compresses them
	 * in the intervalsAndResiduals byte array 
	 * 
	 * @param edgesArray an integer array of successors
	 * */
	private void compress(final int[] edgesArray) {
		try {
			ExtendedDataOutput eos = getConf().createExtendedDataOutput();
			IntArrayList left = new IntArrayList();
			IntArrayList len = new IntArrayList();
			IntArrayList residuals = new IntArrayList();
			// If we are to produce intervals, we first compute them.
			int intervalCount;
			try {
				intervalCount = BVEdges.intervalize(edgesArray, MIN_INTERVAL_LENGTH, MAX_INTERVAL_LENGTH, left, len,
						residuals);
			} catch (IllegalArgumentException e) {
				// array was not sorted, sorting and retrying
				Arrays.sort(edgesArray);
				left = new IntArrayList();
				len = new IntArrayList();
				residuals = new IntArrayList();
				intervalCount = BVEdges.intervalize(edgesArray, MIN_INTERVAL_LENGTH, MAX_INTERVAL_LENGTH, left, len,
						residuals);
			}

			// We write out the intervals.
			eos.writeInt(intervalCount);
			for (int i = 0; i < intervalCount; i++) {
				eos.writeInt(left.getInt(i));
				eos.write(len.getInt(i));
			}
			final int[] residual = residuals.elements();
			final int residualCount = residuals.size();
			for (int i = 0; i < residualCount; i++) {
				eos.writeInt(residual[i]);
			}
			intervalsAndResiduals = eos.toByteArray();
			size = edgesArray.length;
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * Iterator that reuses the same Edge object.
	 */
	private class IntervalResidualEdgeIterator extends UnmodifiableIterator<Edge<IntWritable, NullWritable>> {
		private ExtendedDataInput extendedDataInput = getConf().createExtendedDataInput(intervalsAndResiduals, 0,
				intervalsAndResiduals.length);
		/** Representative edge object. */
		private final Edge<IntWritable, NullWritable> representativeEdge = EdgeFactory.create(new IntWritable());
		/** Current edge count */
		private int currentEdge = 0;
		private int currentLeft;
		private int currentLen = 0;
		private int intervalCount;

		public IntervalResidualEdgeIterator() {
			try {
				intervalCount = extendedDataInput.readInt();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public boolean hasNext() {
			return currentEdge < size;
		}

		@Override
		public Edge<IntWritable, NullWritable> next() {
			this.currentEdge++;
			switch (this.currentLen) {
			case 0:
				switch (this.intervalCount) {
				case 0:
					try {
						representativeEdge.getTargetVertexId().set(extendedDataInput.readInt());
					} catch (IOException canthappen) {
						canthappen.printStackTrace();
					}
					return representativeEdge;
				default:
					try {
						this.currentLeft = extendedDataInput.readInt();
						this.currentLen = extendedDataInput.readByte() & 0xff;
						intervalCount--;
					} catch (IOException canthappen) {
						canthappen.printStackTrace();
					}
					final int result = this.currentLeft;
					this.currentLen--;
					representativeEdge.getTargetVertexId().set(result);
					return representativeEdge;

				}
			default:
				final int result = ++this.currentLeft;
				this.currentLen--;
				representativeEdge.getTargetVertexId().set(result);
				return representativeEdge;
			}

		}

	}
}
