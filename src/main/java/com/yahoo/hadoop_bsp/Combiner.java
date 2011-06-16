package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface for Combiner
 *
 * @param <I extends Writable> index
 * @param <M extends Writable> message data
 *
 **/
@SuppressWarnings("rawtypes")
public interface Combiner <I extends WritableComparable,
                           V extends Writable,
                           E extends Writable,
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
  void combine(CommunicationsInterface<I, V, E, M> comm,
               I vertex,
               List<M> msgList) throws IOException;
}
