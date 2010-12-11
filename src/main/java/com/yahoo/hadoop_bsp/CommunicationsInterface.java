package com.yahoo.hadoop_bsp;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Basic interface for communication between workers.
 * 
 *
 * @param <I extends Writable> vertex id
 * @param <M extends Writable> message data
 *
 **/

public interface CommunicationsInterface
      <I extends Writable, M extends Writable>
      extends VersionedProtocol, Closeable {

	/**
	 * Interface Version History
	 * 
	 * 0 - First Version
	 */
	static final long versionID = 0L;
	
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
   * Adds incoming message.
   * 
   * @param vertex
   * @param msg
   * @throws IOException
   */
  void put(I vertex, M msg)
      throws IOException;
  
  /**
   * Adds incoming message list.
   * 
   * @param vertex
   * @param msg
   * @throws IOException
   */
  void put(I vertex, MsgArrayList<M> msgList)
      throws IOException;
  
  /**
   * Flush all outgoing messages.
   *
   * @throws IOException
   */
  void flush()
      throws IOException;
  
  /**
   * Closes all connections.
   *
   * @throws IOException
   */
  void closeConnections()
      throws IOException;

  /**
   * Shuts down.
   */
  void close();

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
   * @return The name of this worker in the format "hostname:port".
   */
  String getName();

}
