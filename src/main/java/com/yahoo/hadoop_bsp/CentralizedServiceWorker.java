package com.yahoo.hadoop_bsp;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * All workers should have access to this centralized service to
 * execute the following methods.
 *
 * @author aching
 */
public interface CentralizedServiceWorker<
    I extends WritableComparable, V, E, M extends Writable> {

    /**
     * Must to be called prior to any other method.
     */
    public void setup();

    /**
     * Get a synchronized map to the partitions and their sorted vertex lists.
     * This could be used to run compute for the vertices or checkpointing.
     *
     * @return map of max vertex index to list of vertices on that vertex range
     */
    public Map<I, List<Vertex<I, V, E, M>>> getMaxIndexVertexLists();

    /**
     * Take all steps prior to actually beginning the computation of a
     * superstep.
     *
     * @return true if part of this superstep, false otherwise
     */
    public boolean startSuperstep();

    /**
     * Report the statistics of each vertex range after the completion of
     * computation.
     *
     * @param maxIndexStatsMap maps max indexes (vertex ranges) to their stats
     *        (# finished, # total)
     * @return true if this is the last superstep, false otherwise
     */
    public boolean finishSuperstep(final Map<I, long []> maxIndexStatsMap);

    /**
     * Get the partition set.
     * @return partition set
     */
    public SortedSet<Partition<I>> getPartitionSet();

    /**
     * Every client will need to get a partition for an index
     */
    public Partition<I> getPartition(I index);

    /**
     * Get the current global superstep of the application to work on.
     *
     * @return global superstep (begins at -1)
     */
    public long getSuperstep();

    /**
     * Get the total vertices in the entire application during a given
     * superstep.  Note that this is the number of vertices prior to the
     * superstep starting and does not change during the superstep.
     *
     * @return count of all the vertices (local and non-local together)
     */
    public long getTotalVertices();

    /**
     * Clean up the service (no calls may be issued after this)
     */
    void cleanup();
}
