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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A complete edge, the destination vertex and the edge value.  Can only be one
 * edge with a destination vertex id per edge map.
 *
 * @param <I> Vertex index
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class Edge<I extends WritableComparable, E extends Writable>
        implements Writable, Configurable {
    /** Destination vertex index */
    private I destinationVertexIndex = null;
    /** Edge value */
    private E edgeValue = null;
    /** Configuration - Used to instiantiate classes */
    private Configuration conf = null;

    /**
     * Constructor for reflection
     */
    public Edge() {}

    /**
     * Create the edge with final values
     *
     * @param destinationVertexIndex
     * @param edgeValue
     */
    public Edge(I destinationVertexIndex, E edgeValue) {
        this.destinationVertexIndex = destinationVertexIndex;
        this.edgeValue = edgeValue;
    }

    /**
     * Get the destination vertex index of this edge
     *
     * @return Destination vertex index of this edge
     */
    public I getDestinationVertexIndex() {
        return destinationVertexIndex;
    }

    /**
     * Get the edge value of the edge
     *
     * @return Edge value of this edge
     */
    public E getEdgeValue() {
        return edgeValue;
    }

    @Override
    public String toString() {
        return "(DestVertexIndex = " + destinationVertexIndex +
            ", edgeValue = " + edgeValue  + ")";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput input) throws IOException {
        destinationVertexIndex = (I) BspUtils.createVertexIndex(getConf());
        destinationVertexIndex.readFields(input);
        edgeValue = (E) BspUtils.createEdgeValue(getConf());
        edgeValue.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        if (destinationVertexIndex == null) {
            throw new IllegalStateException(
                "write: Null destination vertex index");
        }
        if (edgeValue == null) {
            throw new IllegalStateException(
                "write: Null edge value");
        }
        destinationVertexIndex.write(output);
        edgeValue.write(output);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
