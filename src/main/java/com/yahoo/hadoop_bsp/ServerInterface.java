package com.yahoo.hadoop_bsp;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Public interface for message communication server
 *
 *
 * @param <I extends Writable> vertex id
 * @param <M extends Writable> message data
 *
 **/
@SuppressWarnings("rawtypes")
public interface ServerInterface <I extends WritableComparable,
                                  M extends Writable>
                                  extends Closeable {

    /**
     * Prepare next superstep.
     * In particular, move transient inMessages
     * to the inMessages used for next superstep.
     *
     */
    void prepareSuperstep();

    /**
     * Sends a message to destination vertex.
     *
     * @param destVertex
     * @param msg
     */
    void sendMessage(I destVertex, M msg);

    /**
     * Flush all outgoing messages.
     *
     * @throws IOException
     */
    void flush(Context context)
    throws IOException;

    /**
     * Closes all connections.
     *
     * @throws IOException
     */
    void closeConnections()
    throws IOException;

    /**
     * @return A message iterator for the Worker's received message queue,
     * @throws IOException
     */
    Iterator<Entry<I, ArrayList<M>>> getMessageIterator() throws IOException;

    /**
     * Get the message iterator for a particular vertex
     */
    Iterator<M> getVertexMessageIterator(I vertex);

    /**
     * @return The number of messages in the received message queue.
     */
    int getNumCurrentMessages() throws IOException;

    /**
     * Shuts down.
     */
    void close();
}
