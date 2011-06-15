package com.yahoo.hadoop_bsp;

import java.util.Iterator;
import java.util.List;

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
    void preApplication() throws InstantiationException, IllegalAccessException;

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
     */
    void compute(Iterator<M> msgIterator);

    /**
     * Retrieves the BSP superstep.
     * @return BSP superstep
     */
    long getSuperstep();

    /**
     * Get the vertex id
     */
    I getVertexId();

    /**
     * Get the vertex data
     * @return vertex data
     */
    V getVertexValue();

    /**
     * Set the vertex data (immediately visible in the computation)
     * @param vertexValue Vertex data to be set
     */
    void setVertexValue(V vertexValue);

    /**
     * Get the total number of vertices
     * @return total number of vertices
     */
    long getNumVertices();

    /**
     * Every vertex has edges to other vertices.  Get a handle to the outward
     * edges and their vertices.
     * @return iterator to the outward edges and their destination vertices
     */
    OutEdgeIterator<I, E> getOutEdgeIterator();

    /**
     * Send a message to a vertex id.
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
     *  Get the list of incoming messages from the previous superstep.
     */
    List<M> getMsgList();
}
