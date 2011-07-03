/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
 * Interface for Aggregator.  Allows aggregate operations for all vertices
 * in a given superstep.
 *
 * @param <A extends Writable> Aggregated value
 */
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
