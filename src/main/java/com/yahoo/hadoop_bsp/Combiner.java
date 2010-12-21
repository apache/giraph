package com.yahoo.hadoop_bsp;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Interface for Combiner
 *
 * @param <I extends Writable> index
 * @param <M extends Writable> message data
 *
 **/

public interface Combiner <I extends Writable,
                           M extends Writable> {

  /**
   * Combines message values.
   * Combined message msg should be put out by calling
   * comm.put(vertex, msg).
   * 
   * @param comm
   * @param vertex
   * @param msgs
   * @throws IOException
   */
  void combine(CommunicationsInterface<I, M> comm, I vertex, ArrayList<M> msgList)
      throws IOException;
  
}
