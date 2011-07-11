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
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implement to output a vertex range of the graph after the computation
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface VertexWriter<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable> {
    /**
     * Use the context to setup writing the vertices.
     * Guaranteed to be called prior to any other function.
     *
     * @param context
     * @throws IOException
     */
    void initialize(TaskAttemptContext context) throws IOException;

    /**
     * Writes the next vertex and associated data
     *
     * @param vertex set the properties of this vertex
     * @throws IOException
     * @throws InterruptedException
     */
    void writeVertex(BasicVertex<I, V, E, ?> vertex)
        throws IOException, InterruptedException;

    /**
     * Close this {@link VertexWriter} to future operations.
     *
     * @param context the context of the task
     * @throws IOException
     * @throws InterruptedException
     */
    void close(TaskAttemptContext context)
        throws IOException, InterruptedException;
}
