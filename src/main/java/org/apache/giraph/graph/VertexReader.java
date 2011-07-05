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

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

@SuppressWarnings("rawtypes")
public interface VertexReader<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable> {
    /**
     * Use the input split and context to setup reading the vertices.
     * Guaranteed to be called prior to any other function.
     *
     * @param inputSplit
     * @param context
     * @throws IOException
     */
    void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException;

    /**
     * Reads the next vertex and associated data
     *
     * @param vertex set the properties of this vertex
     * @return true iff a vertex and associated data was read, false if at EOF
     */
    boolean next(MutableVertex<I, V, E, ?> vertex) throws IOException;

    /**
     * Creates and initializes a new vertex id
     *
     * @return new vertex id
     */
    I createVertexId();

    /**
     * Creates and initializes a new vertex value
     *
     * @return new vertex value
     */
    V createVertexValue();

    /**
     * Creates and initializes a new edge value
     *
     * @return new edge value
     */
    E createEdgeValue();

    /**
     * Returns the current position in the input.
     *
     * @return the current position in the input.
     * @throws IOException
     */
    long getPos() throws IOException;

    /**
     * Close this {@link VertexReader} to future operations.
     *
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * How much of the input has the {@link VertexReader} consumed i.e.
     * has been processed by?
     *
     * @return progress from <code>0.0</code> to <code>1.0</code>.
     * @throws IOException
     */
    float getProgress() throws IOException;
}
