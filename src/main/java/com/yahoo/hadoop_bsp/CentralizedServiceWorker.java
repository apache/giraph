package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.yahoo.hadoop_bsp.BspJob.BspMapper;

/**
 * All workers should have access to this centralized service to
 * execute the following methods.
 *
 * @author aching
 */
@SuppressWarnings("rawtypes")
public interface CentralizedServiceWorker<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends CentralizedService, AggregatorUsage {
    /**
     * Get the hostname of this worker
     *
     * @return hostname of this worker
     */
    String getHostname();

    /**
     * Get the port of the RPC server on this worker.
     *
     * @return RPC server of this worker
     */
    int getPort();

    /**
     * Get a synchronized map to the partitions and their sorted vertex lists.
     * This could be used to run compute for the vertices or checkpointing.
     *
     * @return map of max vertex index to list of vertices on that vertex range
     */
    NavigableMap<I, VertexRange<I, V, E, M>> getVertexRangeMap();

    /**
     * Get the current map to the partitions and their sorted vertex lists.
     * This is needed by the communication service to shift incoming messages
     * to the vertex lists before the new map gets synchronized.
     *
     * @return map of max vertex index to list of vertices on that vertex range
     */
    NavigableMap<I, VertexRange<I, V, E, M>> getCurrentVertexRangeMap();

    /**
     *  Both the vertices and the messages need to be checkpointed in order
     *  for them to be used.  This is done after all messages have been
     *  delivered, but prior to a superstep starting.
     */
    void storeCheckpoint() throws IOException;

    /**
     * Load the vertices, edges, messages from the beginning of a superstep.
     * Will load the vertex partitions as designated by the master and set the
     * appropriate superstep.
     *
     * @param superstep which checkpoint to use
     * @throws IOException
     */
    void loadCheckpoint(long superstep) throws IOException;

    /**
     * Take all steps prior to actually beginning the computation of a
     * superstep.
     *
     * @return true if part of this superstep, false otherwise
     */
    boolean startSuperstep();

    /**
     * Report the statistics of each vertex range after the completion of
     * computation.
     *
     * @param maxIndexStatsMap maps max indexes (vertex ranges) to their stats
     *        (# finished, # total)
     * @return true if this is the last superstep, false otherwise
     */
    boolean finishSuperstep(final Map<I, long []> maxIndexStatsMap);

    /**
     * Every client will need to get a vertex range for a vertex id
     */
    VertexRange<I, V, E, M> getVertexRange(I index);

    /**
     * Get the total vertices in the entire application during a given
     * superstep.  Note that this is the number of vertices prior to the
     * superstep starting and does not change during the superstep.
     *
     * @return count of all the vertices (local and non-local together)
     */
    long getTotalVertices();

    /**
     * If desired by the user, vertex ranges are redistributed among workers
     * according to the chosen {@link VertexRangeBalancer}.
     */
    void exchangeVertexRanges();

    /**
     * Get the BspMapper that this service is using.  Vertices need to know
     * this.
     *
     * @return BspMapper
     */
    BspMapper<I, V, E, M> getBspMapper();
}
