/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;

/**
 * Wrapper for aggregators. Keeps two instances of an aggregator - one for
 * the value from previous super step, and one for the value which is being
 * generated in current super step.
 *
 * @param <A> Aggregated value
 */
public class AggregatorWrapper<A extends Writable> {
  /** False iff aggregator should be reset at the end of each super step */
  private final boolean persistent;
  /** Value aggregated in previous super step */
  private A previousAggregatedValue;
  /** Aggregator for next super step */
  private final Aggregator<A> currentAggregator;
  /** Whether anyone changed current value since the moment it was reset */
  private boolean changed;

  /**
   * @param aggregatorClass Class type of the aggregator
   * @param persistent      False iff aggregator should be reset at the end of
   *                        each super step
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public AggregatorWrapper(Class<? extends Aggregator<A>> aggregatorClass,
      boolean persistent) throws IllegalAccessException,
      InstantiationException {
    this.persistent = persistent;
    currentAggregator = aggregatorClass.newInstance();
    changed = false;
    previousAggregatedValue = currentAggregator.createInitialValue();
  }

  /**
   * Get aggregated value from previous super step
   *
   * @return Aggregated value from previous super step
   */
  public A getPreviousAggregatedValue() {
    return previousAggregatedValue;
  }

  /**
   * Set aggregated value for previous super step
   *
   * @param value Aggregated value to set
   */
  public void setPreviousAggregatedValue(A value) {
    previousAggregatedValue = value;
  }

  /**
   * Check if aggregator is persistent
   *
   * @return False iff aggregator should be reset at the end of each super step
   */
  public boolean isPersistent() {
    return persistent;
  }

  /**
   * Check if current aggregator was changed
   *
   * @return Whether anyone changed current value since the moment it was reset
   */
  public boolean isChanged() {
    return changed;
  }

  /**
   * Add a new value to current aggregator
   *
   * @param value Value to be aggregated
   */
  public void aggregateCurrent(A value) {
    changed = true;
    currentAggregator.aggregate(value);
  }

  /**
   * Get current aggregated value
   *
   * @return Current aggregated value
   */
  public A getCurrentAggregatedValue() {
    return currentAggregator.getAggregatedValue();
  }

  /**
   * Set aggregated value of current aggregator
   *
   * @param value Value to set it to
   */
  public void setCurrentAggregatedValue(A value) {
    changed = true;
    currentAggregator.setAggregatedValue(value);
  }

  /**
   * Reset the value of current aggregator to neutral value
   */
  public void resetCurrentAggregator() {
    changed = false;
    currentAggregator.reset();
  }

  /**
   * Return new aggregated value which is neutral to aggregate operation
   *
   * @return Neutral value
   */
  public A createInitialValue() {
    return currentAggregator.createInitialValue();
  }

  /**
   * Get class of wrapped aggregator
   *
   * @return Aggregator class
   */
  public Class<? extends Aggregator> getAggregatorClass() {
    return currentAggregator.getClass();
  }
}
