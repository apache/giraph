/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * Vertex classes can use this interface to register and use aggregators
 *
 * @param <A> Aggregator type must extend Writable
 */
public interface AggregatorUsage {
    /**
     * Register an aggregator in preSuperstep() and/or preApplication().
     *
     * @param name of aggregator
     * @param aggregatorClass Class type of the aggregator
     * @return created Aggregator or null when already registered
     */
    public <A extends Writable> Aggregator<A> registerAggregator(
        String name,
        Class<? extends Aggregator<A>> aggregatorClass)
        throws InstantiationException, IllegalAccessException;

    /**
     * Get a registered aggregator.
     *
     * @param name of aggregator
     * @return Aggregator<A> (null when not registered)
     */
    public Aggregator<? extends Writable> getAggregator(String name);

    /**
     * Use a registered aggregator in current superstep.
     * Even when the same aggregator should be used in the next
     * superstep, useAggregator needs to be called at the beginning
     * of that superstep in preSuperstep().
     *
     * @param name of aggregator
     * @return boolean (false when not registered)
     */
    public boolean useAggregator(String name);
}
