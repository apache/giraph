package com.yahoo.hadoop_bsp;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.io.Writable;

/**
 * Basic interface for communication between workers.
 * 
 *
 * @param <I extends Writable> vertex id
 * @param <M extends Writable> message data
 *
 **/

public interface CommunicationsInterface<I, M>
      extends VersionedProtocol, Closeable {

	/**
	 * Interface Version History
	 * 
	 * 0 - First Version
	 */
	static final long versionID = 0L;
	
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
   * Flush all outgoing messages.
   */
  void flush();
  
  /**
   * @return A message iterator for the Worker's received message queue,
   * @throws IOException
   */
  Iterator<Entry<I, ArrayList<M>>> getMessageIterator() throws IOException;

  /**
   * @return The number of messages in the received message queue.
   */
  int getNumCurrentMessages() throws IOException;
  
  /**
   * @return The name of this worker in the format "hostname:port".
   */
  String getName();

}
