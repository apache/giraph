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

import java.util.NavigableMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Simple balancer that maintains the distribution of vertex ranges that are
 * created by the InputSplit processing.  Vertex ranges never change from the
 * initial configuration and are hence, static.
 */
@SuppressWarnings("rawtypes")
public final class StaticBalancer<I extends WritableComparable,
                                  V extends Writable,
                                  E extends Writable,
                                  M extends Writable>
        extends VertexRangeBalancer<I, V, E, M> {

    @Override
    public final NavigableMap<I, VertexRange<I, V, E, M>> rebalance() {
        return getPrevVertexRangeMap();
    }
}
