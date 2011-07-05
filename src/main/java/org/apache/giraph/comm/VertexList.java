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

package org.apache.giraph.comm;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Wrapper around {@link ArrayListWritable} that allows the vertex
 * class to be set prior to calling the default constructor.
 *
 * @param <H> Hadoop Vertex type
 */
@SuppressWarnings("rawtypes")
public class VertexList<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends ArrayListWritable<Vertex<I, V, E, M>> {
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 1000L;

    /**
     * Default constructor for reflection
     */
    public VertexList() {}

    @SuppressWarnings("unchecked")
    @Override
    public void setClass() {
        setClass((Class<Vertex<I, V, E, M>>)
                 BspUtils.<I, V, E, M>getVertexClass(getConf()));
    }
}
