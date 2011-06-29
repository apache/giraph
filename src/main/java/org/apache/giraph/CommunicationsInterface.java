package org.apache.giraph;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.token.TokenInfo;

/**
 * Basic interface for communication between workers.
 *
 *
 * @param <I extends Writable> vertex id
 * @param <M extends Writable> message data
 *
 **/
@SuppressWarnings("rawtypes")
@TokenInfo(BspTokenSelector.class)
public interface CommunicationsInterface<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends VersionedProtocol {

    /**
     * Interface Version History
     *
     * 0 - First Version
     */
    static final long versionID = 0L;

    /**
     * Adds incoming message.
     *
     * @param vertexIndex
     * @param msg
     * @throws IOException
     */
    void putMsg(I vertexIndex, M msg) throws IOException;

    /**
     * Adds incoming message list.
     *
     * @param vertexIndex Vertex index where the message are added
     * @param msgList messages added
     * @throws IOException
     */
    void putMsgList(I vertexIndex, MsgList<M> msgList) throws IOException;

    /**
     * Adds vertex list (index, value, edges, etc.) to the appropriate worker.
     *
     * @param vertexRangeIndex
     */
    void putVertexList(I vertexIndexMax,
                       HadoopVertexList<I, V, E, M> vertexList)
        throws IOException;

    /**
     * Add an edge to a remote vertex
     *
     * @param vertexIndex Vertex index where the edge is added
     * @param edge Edge to be added
     * @throws IOException
     */
    void addEdge(I vertexIndex, Edge<I, E> edge) throws IOException;

    /**
     * Remove an edge on a remote vertex
     *
     * @param vertexIndex Vertex index where the edge is added
     * @param destinationVertexIndex Edge vertex index to be removed
     * @throws IOException
     */
    void removeEdge(I vertexIndex, I destinationVertexIndex) throws IOException;

    /**
     * Add a remote vertex
     *
     * @param vertex Vertex that will be added
     * @throws IOException
     */
    void addVertex(MutableVertex<I, V, E, M> vertex) throws IOException;

    /**
     * Removed a remote vertex
     *
     * @param vertexIndex Vertex index representing vertex to be removed
     * @throws IOException
     */
    void removeVertex(I vertexIndex) throws IOException;

    /**
     * @return The name of this worker in the format "hostname:port".
     */
    String getName();
}
