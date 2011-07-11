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

package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Abstract class that users should subclass to use their own text based
 * vertex output format.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class TextVertexOutputFormat<
        I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexOutputFormat<I, V, E> {
    /** Uses the TextOutputFormat to do everything */
    protected TextOutputFormat<Text, Text> textOutputFormat =
        new TextOutputFormat<Text, Text>();

    /**
     * Abstract class to be implemented by the user based on their specific
     * vertex output.  Easiest to ignore the key value separator and only use
     * key instead.
     *
     * @param <I> Vertex index value
     * @param <V> Vertex value
     * @param <E> Edge value
     */
    public static abstract class TextVertexWriter<I extends WritableComparable,
            V extends Writable, E extends Writable>
            implements VertexWriter<I, V, E> {
        /** Internal line record reader */
        private final RecordWriter<Text, Text> lineRecordWriter;

        /**
         * Initialize with the LineRecordWriter.
         *
         * @param lineRecordWriter Line record writer from TextOutputFormat
         */
        public TextVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            this.lineRecordWriter = lineRecordWriter;
        }

        @Override
        public void initialize(TaskAttemptContext context) throws IOException {
        }

        @Override
        public void close(TaskAttemptContext context)
                throws IOException, InterruptedException {
            lineRecordWriter.close(context);
        }

        /**
         * Get the record writer.
         *
         * @return Record writer to be used for writing.
         */
        public RecordWriter<Text, Text> getRecordWriter() {
            return lineRecordWriter;
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context)
            throws IOException, InterruptedException {
        textOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return textOutputFormat.getOutputCommitter(context);
    }
}
