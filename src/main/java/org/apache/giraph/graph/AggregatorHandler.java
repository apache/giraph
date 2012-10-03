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

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Class which handles all the actions with aggregators
 */
public abstract class AggregatorHandler {
  /** Map of aggregators */
  private final Map<String, AggregatorWrapper<Writable>> aggregatorMap;

  /**
   * Default constructor
   */
  protected AggregatorHandler() {
    aggregatorMap = Maps.newHashMap();
  }

  /**
   * Get value of an aggregator.
   *
   * @param name Name of aggregator
   * @param <A> Aggregated value
   * @return Value of the aggregator
   */
  public <A extends Writable> A getAggregatedValue(String name) {
    AggregatorWrapper<? extends Writable> aggregator = getAggregator(name);
    if (aggregator == null) {
      return null;
    } else {
      return (A) aggregator.getPreviousAggregatedValue();
    }
  }

  /**
   * Get aggregator by name.
   *
   * @param name Name of aggregator
   * @return Aggregator or null when not registered
   */
  protected AggregatorWrapper<Writable> getAggregator(String name) {
    return aggregatorMap.get(name);
  }

  /**
   * Get map of aggregators
   *
   * @return Aggregators map
   */
  protected Map<String, AggregatorWrapper<Writable>> getAggregatorMap() {
    return aggregatorMap;
  }

  /**
   * Register an aggregator with name and class.
   *
   * @param <A> Aggregator type
   * @param name Name of the aggregator
   * @param aggregatorClass Class of the aggregator
   * @param persistent False iff aggregator should be reset at the end of
   *                   every super step
   * @return Aggregator
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  protected  <A extends Writable> AggregatorWrapper<A> registerAggregator(
      String name, Class<? extends Aggregator<A>> aggregatorClass,
      boolean persistent) throws InstantiationException,
      IllegalAccessException {
    if (getAggregator(name) != null) {
      return null;
    }
    AggregatorWrapper<A> aggregator =
        new AggregatorWrapper<A>(aggregatorClass, persistent);
    AggregatorWrapper<Writable> writableAggregator =
        (AggregatorWrapper<Writable>) aggregator;
    aggregatorMap.put(name, writableAggregator);
    return aggregator;
  }

  /**
   * Register an aggregator with name and className.
   *
   * @param <A> Aggregator type
   * @param name Name of the aggregator
   * @param aggregatorClassName Name of the aggregator class
   * @param persistent False iff aggregator should be reset at the end of
   *                   every super step
   * @return Aggregator
   */
  protected <A extends Writable> AggregatorWrapper<A> registerAggregator(
      String name, String aggregatorClassName, boolean persistent) {
    AggregatorWrapper<Writable> aggregatorWrapper = getAggregator(name);
    if (aggregatorWrapper == null) {
      try {
        Class<? extends Aggregator<Writable>> aggregatorClass =
            (Class<? extends Aggregator<Writable>>)
                Class.forName(aggregatorClassName);
        aggregatorWrapper =
            registerAggregator(name, aggregatorClass, persistent);
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Failed to create aggregator " +
            name + " of class " + aggregatorClassName +
            " with ClassNotFoundException", e);
      } catch (InstantiationException e) {
        throw new IllegalStateException("Failed to create aggregator " +
            name + " of class " + aggregatorClassName +
            " with InstantiationException", e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Failed to create aggregator " +
            name + " of class " + aggregatorClassName +
            " with IllegalAccessException", e);
      }
    }
    return (AggregatorWrapper<A>) aggregatorWrapper;
  }
}
