package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;

/**
 * Interface for Aggregator
 *
 * @param <A extends Writable> aggregated value
 **/

public interface Aggregator<A extends Writable> {

  /**
   * Add a new value.
   * Needs to be commutative and associative
   * 
   * @param value
   */
  void aggregate(A value);

  /**
   * Set aggregated value.
   * Can be used for initialization or reset.
   * 
   * @param value
   */
  void setAggregatedValue(A value);

  /**
   * Return current aggregated value.
   * Needs to be initialized if aggregate or setAggregatedValue
   * have not been called before.
   * 
   * @return A
   */
  A getAggregatedValue();

  /**
   * Return new aggregated value.
   * Must be changeable without affecting internals of Aggregator
   * 
   * @return Writable
   */
  A createAggregatedValue();
  
}
