package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface for Combiner
 *
 * @param <I extends Writable> index
 * @param <M extends Writable> message data
 *
 **/

public interface Combiner <I extends WritableComparable,
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
