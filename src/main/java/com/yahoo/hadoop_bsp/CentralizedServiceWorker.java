package com.yahoo.hadoop_bsp;

import java.io.IOException;
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
@SuppressWarnings("rawtypes")
public interface CentralizedServiceWorker<I extends WritableComparable,
                                          V extends Writable,
                                          E extends Writable,
                                          M extends Writable> extends
                                          CentralizedService {
    /**
     * Get a synchronized map to the partitions and their sorted vertex lists.
     * This could be used to run compute for the vertices or checkpointing.
     *
     * @return map of max vertex index to list of vertices on that vertex range
     */
    public Map<I, List<Vertex<I, V, E, M>>> getMaxIndexVertexLists();

    /**
     *  Both the vertices and the messages need to be checkpointed in order
     *  for them to be used.  This is done after all messages have been
     *  delivered, but prior to a superstep starting.
     */
    public void storeCheckpoint() throws IOException;

    /**
     * Load the vertices, edges, messages from the beginning of a superstep.
     * Will load the vertex partitions as designated by the master and set the
     * appropriate superstep.
     *
     * @param superstep which checkpoint to use
     * @throws IOException
     */
    public void loadCheckpoint(long superstep) throws IOException;

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
     * Get the vertex range set.
     *
     * @param superstep use this superstep's vertex range
     * @return vertex range set
     */
    public SortedSet<VertexRange<I>> getVertexRangeSet(long superstep);

    /**
     * Every client will need to get a vertex range for a vertex id
     */
    public VertexRange<I> getVertexRange(I index);

    /**
     * Get the total vertices in the entire application during a given
     * superstep.  Note that this is the number of vertices prior to the
     * superstep starting and does not change during the superstep.
     *
     * @return count of all the vertices (local and non-local together)
     */
    public long getTotalVertices();
}
