package org.apache.giraph.edge;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.AbstractLazyIntIterator;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.IntIntervalSequenceIterator;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterators;
import it.unimi.dsi.webgraph.MergedIntIterator;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.weakref.jmx.com.google.common.collect.Iterators;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;

public class BVEdges extends ConfigurableOutEdges<IntWritable, NullWritable>
		implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {

	/** Serialized Intervals and Residuals */
	private byte[] intervalsAndResiduals;
	/** Number of edges. */
	private int size;

	@Override
	public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
		compress(StreamSupport.stream(edges.spliterator(), false).map(Edge::getTargetVertexId)
				.mapToInt(IntWritable::get).toArray());
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
		// Note that ths is very expensive (decompresses all edges and recompresses them again).
		compress(StreamSupport
				.stream(((Iterable<Edge<IntWritable, NullWritable>>) () -> Iterators.concat(this.iterator(),
						ImmutableSet.of(edge).iterator())).spliterator(), false)
				.map(Edge::getTargetVertexId).mapToInt(IntWritable::get).sorted().toArray());
	}

	@Override
	public void remove(IntWritable targetVertexId) {
		// Note that this is very expensive (decompresses all edges and recompresses them again).
		initialize(Iterables.filter(this, edge -> !edge.getTargetVertexId().equals(targetVertexId)));
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
			return new BVEdgesIterator();
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
		// Nothing to do
	}


	/**
	 * Receives an integer array of successors and compresses them in the
	 * intervalsAndResiduals byte array
	 * 
	 * @param edgesArray an integer array of successors
	 */
	private void compress(final int[] edgesArray) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			diffComp(edgesArray, new OutputBitStream(baos));
			intervalsAndResiduals = baos.toByteArray();
			size = edgesArray.length;
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * See also {@link it.unimi.dsi.webgraph.BVGraph#diffComp(int[], OutputBitStream)}.
	 * This method is given an integer successors array and produces, onto a given output
	 * bit stream, the differentially compressed successor list.
	 *
	 * @param edgesArray the integer successors array
	 * @param obs        an output bit stream where the compressed data will be
	 *                   stored.
	 */
	public static void diffComp(final int[] edgesArray, OutputBitStream obs) throws IOException {
		// We write the degree.
		obs.writeInt(edgesArray.length, Integer.SIZE);
		IntArrayList left = new IntArrayList();
		IntArrayList len = new IntArrayList();
		IntArrayList residuals = new IntArrayList();
		// If we are to produce intervals, we first compute them.
		int intervalCount;
		try {
			intervalCount = BVEdges.intervalize(edgesArray, BVGraph.DEFAULT_MIN_INTERVAL_LENGTH, Integer.MAX_VALUE,
					left, len, residuals);
		} catch (IllegalArgumentException e) {
			// array was not sorted, sorting and retrying
			Arrays.sort(edgesArray);
			left = new IntArrayList();
			len = new IntArrayList();
			residuals = new IntArrayList();
			intervalCount = BVEdges.intervalize(edgesArray, BVGraph.DEFAULT_MIN_INTERVAL_LENGTH, Integer.MAX_VALUE,
					left, len, residuals);
		}
		// We write the number of intervals.
		obs.writeGamma(intervalCount);
		int currIntLen;
		int prev = 0;
		if (intervalCount > 0) {
			obs.writeInt(left.getInt(0), Integer.SIZE);
			currIntLen = len.getInt(0);
			prev = left.getInt(0) + currIntLen;
			obs.writeGamma(currIntLen - BVGraph.DEFAULT_MIN_INTERVAL_LENGTH);
		}
		// We write out the intervals.
		for (int i = 1; i < intervalCount; i++) {
			obs.writeGamma(left.getInt(i) - prev - 1);
			currIntLen = len.getInt(i);
			prev = left.getInt(i) + currIntLen;
			obs.writeGamma(currIntLen - BVGraph.DEFAULT_MIN_INTERVAL_LENGTH);
		}
		final int[] residual = residuals.elements();
		final int residualCount = residuals.size();
		// Now we write out the residuals, if any
		if (residualCount != 0) {
			if (intervalCount > 0) {
				obs.writeLongZeta(Fast.int2nat((long) (prev = residual[0]) - left.getInt(0)), BVGraph.DEFAULT_ZETA_K);
			} else {
				prev = residual[0];
				obs.writeInt(prev, Integer.SIZE);
			}
			for (int i = 1; i < residualCount; i++) {
				if (residual[i] == prev) {
					throw new IllegalArgumentException(
							"Repeated successor " + prev + " in successor list of this node");
				}
				obs.writeLongZeta(residual[i] - prev - 1L, BVGraph.DEFAULT_ZETA_K);
				prev = residual[i];
			}
		}
		obs.flush();
	}

	/**
	 * See also
	 * {@link it.unimi.dsi.webgraph.BVGraph#intervalize(IntArrayList, int, IntArrayList, IntArrayList, IntArrayList)}.
	 * This method tries to express an increasing sequence of natural numbers
	 * <code>x</code> as a union of an increasing sequence of intervals and an
	 * increasing sequence of residual elements. More precisely, this
	 * intervalization works as follows: first, one looks at <code>edgesArray</code>
	 * as a sequence of intervals (i.e., maximal sequences of consecutive elements);
	 * those intervals whose length is &ge; <code>minInterval</code> are stored in
	 * the lists <code>left</code> (the list of left extremes) and <code>len</code>
	 * (the list of lengths; the length of an integer interval is the number of
	 * integers in that interval). The remaining integers, called <em>residuals</em>
	 * are stored in the <code>residual</code> list.
	 * 
	 * <P>
	 * Note that the previous content of <code>left</code>, <code>len</code> and
	 * <code>residual</code> is lost.
	 * 
	 * @param edgesArray  the array to be intervalized (an increasing list of
	 *                    natural numbers).
	 * @param minInterval the least length that a maximal sequence of consecutive
	 *                    elements must have in order for it to be considered as an
	 *                    interval.
	 * @param left        the resulting list of left extremes of the intervals.
	 * @param len         the resulting list of interval lengths.
	 * @param residuals   the resulting list of residuals.
	 * @return the number of intervals.
	 */
	protected static int intervalize(final int[] edgesArray, final int minInterval,
			final int maxInterval, final IntArrayList left, final IntArrayList len, final IntArrayList residuals) {
		int nInterval = 0;
		int i;
		int j;

		for (i = 0; i < edgesArray.length; i++) {
			j = 0;
			checkIsSorted(edgesArray, i);
			if (i < edgesArray.length - 1 && edgesArray[i] + 1 == edgesArray[i + 1]) {
				do
					j++;
				while (i + j < edgesArray.length - 1 && j < maxInterval
						&& edgesArray[i + j] + 1 == edgesArray[i + j + 1]);
				checkIsSorted(edgesArray, i + j);
				j++;
				// Now j is the number of integers in the interval.
				if (j >= minInterval) {
					left.add(edgesArray[i]);
					len.add(j);
					nInterval++;
					i += j - 1;
				}
			}
			if (j < minInterval)
				residuals.add(edgesArray[i]);
		}
		return nInterval;
	}

	/**
	 * Given an integer array and an index, this method throws an exception in case
	 * the element of the array the index points at is equal or larger than its
	 * next.
	 * 
	 * @param edgesArray the integer array
	 * @param i          the index
	 */
	private static void checkIsSorted(int[] edgesArray, int i) {
		if (i < edgesArray.length - 1 && edgesArray[i] == edgesArray[i + 1]) {
			throw new IllegalArgumentException("Parallel edges are not allowed.");
		}
		if (i < edgesArray.length - 1 && edgesArray[i] > edgesArray[i + 1]) {
			throw new IllegalArgumentException("Edges are not sorted.");
		}
	}

	/**
	 * Iterator that reuses the same Edge object.
	 */
	private class BVEdgesIterator extends UnmodifiableIterator<Edge<IntWritable, NullWritable>> {
		/** Wrapped map iterator. */
		LazyIntIterator liIter = successors(new InputBitStream(intervalsAndResiduals));
		/** Representative edge object. */
		private final Edge<IntWritable, NullWritable> representativeEdge = EdgeFactory.create(new IntWritable());
		/** Current edge count */
		private int currentEdge = 0;

		@Override
		public boolean hasNext() {
			return currentEdge < size;
		}

		@Override
		public Edge<IntWritable, NullWritable> next() {
			representativeEdge.getTargetVertexId().set(liIter.nextInt());
			currentEdge++;
			return representativeEdge;
		}

		private LazyIntIterator successors(InputBitStream ibs) {
			try {
				final int d;
				int extraCount;
				int firstIntervalNode = -1;
				ibs.position(0);
				d = ibs.readInt(Integer.SIZE);
				if (d == 0)
					return LazyIntIterators.EMPTY_ITERATOR;
				extraCount = d;
				int intervalCount = 0; // Number of intervals
				int[] left = null;
				int[] len = null;
				// Prepare to read intervals, if any
				if (extraCount > 0 && (intervalCount = ibs.readGamma()) != 0) {
					int prev = 0; // Holds the last integer in the last
									// interval.
					left = new int[intervalCount];
					len = new int[intervalCount];
					// Now we read intervals
					left[0] = firstIntervalNode = prev = ibs.readInt(Integer.SIZE);
					len[0] = ibs.readGamma() + BVGraph.DEFAULT_MIN_INTERVAL_LENGTH;

					prev += len[0];
					extraCount -= len[0];
					for (int i = 1; i < intervalCount; i++) {
						left[i] = prev = ibs.readGamma() + prev + 1;
						len[i] = ibs.readGamma() + BVGraph.DEFAULT_MIN_INTERVAL_LENGTH;
						prev += len[i];
						extraCount -= len[i];
					}
				}

				final int residualCount = extraCount; // Just to be able to use
														// an
														// anonymous class.
				final LazyIntIterator residualIterator = residualCount == 0 ? null
						: new ResidualIntIterator(ibs, residualCount, firstIntervalNode);
				// The extra part is made by the contribution of intervals, if
				// any, and by the residuals iterator.
				if (intervalCount == 0) {
					return residualIterator;
				} else if (residualCount == 0){
					return new IntIntervalSequenceIterator(left, len);
				} else {
					return new MergedIntIterator(new IntIntervalSequenceIterator(left, len), residualIterator);
				}
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		/** An iterator returning the residuals of a node. */
		private final class ResidualIntIterator extends AbstractLazyIntIterator {
			/** The input bit stream from which residuals will be read. */
			private final InputBitStream ibs;
			/** The last residual returned. */
			private int next;
			/** The number of remaining residuals. */
			private int remaining;

			private ResidualIntIterator(final InputBitStream ibs, final int residualCount, int x) {
				this.remaining = residualCount;
				this.ibs = ibs;
				try {
					if (x >= 0) {
						long temp = Fast.nat2int(ibs.readLongZeta(BVGraph.DEFAULT_ZETA_K));
						this.next = (int) (x + temp);
					} else {
						this.next = ibs.readInt(Integer.SIZE);
					}
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			public int nextInt() {
				if (remaining == 0)
					return -1;
				try {
					final int result = next;
					if (--remaining != 0)
						next += ibs.readZeta(BVGraph.DEFAULT_ZETA_K) + 1;
					return result;
				} catch (IOException cantHappen) {
					throw new IllegalStateException(cantHappen);
				}
			}

			@Override
			public int skip(int n) {
				if (n >= remaining) {
					n = remaining;
					remaining = 0;
					return n;
				}
				try {
					for (int i = n; i-- != 0;)
						next += ibs.readZeta(BVGraph.DEFAULT_ZETA_K) + 1;
					remaining -= n;
					return n;
				} catch (IOException cantHappen) {
					throw new IllegalStateException(cantHappen);
				}
			}

		}

	}

}
