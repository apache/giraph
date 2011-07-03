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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Use this to load data for a BSP application.  Note that the InputSplit must
 * also implement Writable.  The InputSplits will determine the partitioning of
 * vertices across the mappers, so keep that in consideration when implementing
 * getSplits().
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface VertexInputFormat<I extends WritableComparable,
        V extends Writable, E extends Writable> {
    /**
     * Logically split the vertices for a BSP application.
     *
     * Each {@link InputSplit} is then assigned to a worker for processing.
     *
     * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
     * input files are not physically split into chunks. For e.g. a split could
     * be <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat
     * also creates the {@link VertexReader} to read the {@link InputSplit}.
     *
     * @param conf configuration
     * @param numSplits number of splits for the input
     * @return an array of {@link InputSplit}s for the job.
     */
    List<InputSplit> getSplits(Configuration conf, int numSplits)
        throws IOException, InterruptedException;

    /**
     * Create a vertex reader for a given split. The framework will call
     * {@link VertexReader#initialize(InputSplit, TaskAttemptContext)} before
     * the split is used.
     * @param split the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws IOException
     * @throws InterruptedException
     */
    VertexReader<I, V, E> createVertexReader(InputSplit split,
                                             TaskAttemptContext context)
                                             throws IOException;
}
