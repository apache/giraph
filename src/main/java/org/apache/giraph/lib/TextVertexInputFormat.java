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

package org.apache.giraph.lib;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Abstract class that users should subclass to use their own text based
 * vertex output format.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class TextVertexInputFormat<
        I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexInputFormat<I, V, E> {
    /** Uses the TextInputFormat to do everything */
    protected TextInputFormat textInputFormat = new TextInputFormat();

    /**
     * Abstract class to be implemented by the user based on their specific
     * vertex input.  Easiest to ignore the key value separator and only use
     * key instead.
     *
     * @param <I> Vertex index value
     * @param <V> Vertex value
     * @param <E> Edge value
     */
    public static abstract class TextVertexReader<I extends WritableComparable,
            V extends Writable, E extends Writable>
            implements VertexReader<I, V, E> {
        /** Internal line record reader */
        private final RecordReader<LongWritable, Text> lineRecordReader;
        /** Context passed to initialize */
        private TaskAttemptContext context;

        /**
         * Initialize with the LineRecordReader.
         *
         * @param lineRecordReader Line record reader from TextInputFormat
         */
        public TextVertexReader(RecordReader<LongWritable, Text> recordReader) {
            this.lineRecordReader = recordReader;
        }

        @Override
        public void initialize(InputSplit inputSplit,
                               TaskAttemptContext context)
                               throws IOException, InterruptedException {
            lineRecordReader.initialize(inputSplit, context);
            this.context = context;
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        /**
         * Get the line record reader.
         *
         * @return Record reader to be used for reading.
         */
        public RecordReader<LongWritable, Text> getRecordReader() {
            return lineRecordReader;
        }

        /**
         * Get the context.
         *
         * @return Context passed to initialize.
         */
        public TaskAttemptContext getContext() {
            return context;
        }
    }

    @Override
    public List<InputSplit> getSplits(
            JobContext context, int numWorkers)
            throws IOException, InterruptedException {
        // Ignore the hint of numWorkers here since we are using TextInputFormat
        // to do this for us
        return textInputFormat.getSplits(context);
    }
}
