package org.apache.giraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Basic interface for writing a BSP application for computation.
 *
 * @author aching
 *
 * @param <I> vertex id
 * @param <V> vertex data
 * @param <E> edge data
 * @param <M> message data
 */
@SuppressWarnings("rawtypes")
public interface Vertex<I extends WritableComparable,
                        V extends Writable,
                        E extends Writable,
                        M extends Writable>
                        extends AggregatorUsage {
    /**
     * Optionally defined by the user to be executed once on all workers
     * before application has started.
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    void preApplication()
        throws InstantiationException, IllegalAccessException;

    /**
     * Optionally defined by the user to be executed once on all workers
     * after the application has completed.
     */
    void postApplication();

    /**
     * Optionally defined by the user to be executed once prior to vertex
     * processing on a worker for the current superstep.
     */
    void preSuperstep();

    /**
     * Optionally defined by the user to be executed once after all vertex
     * processing on a worker for the current superstep.
     */
    void postSuperstep();

    /**
     * Creates and initializes a message value.
     */
    M createMsgValue();

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param msgIterator Iterator to the messages that were sent to this
     *        vertex in the previous superstep
     * @throws IOException
     */
    void compute(Iterator<M> msgIterator) throws IOException;

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    long getSuperstep();

    /**
     * Get the vertex id
     */
    I getVertexId();

    /**
     * Get the vertex value (data stored with vertex)
     *
     * @return Vertex value
     */
    V getVertexValue();

    /**
     * Set the vertex data (immediately visible in the computation)
     *
     * @param vertexValue Vertex data to be set
     */
    void setVertexValue(V vertexValue);

    /**
     * Get the total (all workers) number of vertices that
     * existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep)
     */
    long getNumVertices();

    /**
     * Get the total (all workers) number of edges that
     * existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    long getNumEdges();

    /**
     * Every vertex has edges to other vertices.  Get a handle to the outward
     * edge set.
     *
     * @return Map of the destination vertex index to the {@link Edge}
     */
    SortedMap<I, Edge<I, E>> getOutEdgeMap();

    /**
     * Send a message to a vertex id.
     *
     * @param id vertex id to send the message to
     * @param msg message data to send
     */
    void sendMsg(I id, M msg);

    /**
     * Send a message to all edges.
     */
    void sentMsgToAllEdges(M msg);

    /**
     * After this is called, the compute() code will no longer be called for
     * this vertice unless a message is sent to it.  Then the compute() code
     * will be called once again until this function is called.  The application
     * finishes only when all vertices vote to halt.
     */
    void voteToHalt();

    /**
     * Is this vertex done?
     */
    boolean isHalted();

    /**
     *  Get the list of incoming messages from the previous superstep.  Same as
     *  the message iterator passed to compute().
     */
    List<M> getMsgList();
}
