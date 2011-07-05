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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implement to output the graph after the computation.  It is modeled
 * directly after the Hadoop OutputFormat.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface VertexOutputFormat<
        I extends WritableComparable, V extends Writable, E extends Writable> {
    /**
     * Create a vertex writer for a given split. The framework will call
     * {@link VertexReader#initialize(InputSplit, TaskAttemptContext)} before
     * the split is used.
     *
     * @param context the information about the task
     * @return a new vertex writer
     * @throws IOException
     */
    VertexWriter<I, V, E> createVertexWriter(TaskAttemptContext context)
        throws IOException;

    /**
     * Check for validity of the output-specification for the job.
     * (Copied from Hadoop OutputFormat)
     *
     * <p>This is to validate the output specification for the job when it is
     * a job is submitted.  Typically checks that it does not already exist,
     * throwing an exception when it already exists, so that output is not
     * overwritten.</p>
     *
     * @param context information about the job
     * @throws IOException when output should not be attempted
     */
    void checkOutputSpecs(JobContext context)
        throws IOException, InterruptedException;

    /**
     * Get the output committer for this output format. This is responsible
     * for ensuring the output is committed correctly.
     * (Copied from Hadoop OutputFormat)
     *
     * @param context the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    OutputCommitter getOutputCommitter(TaskAttemptContext context)
        throws IOException, InterruptedException;
}
