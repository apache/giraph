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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;

/**
 * Simple text based vertex output format example.
 */
public class SimpleTextVertexOutputFormat extends
         TextVertexOutputFormat<LongWritable, IntWritable, FloatWritable> {
    /**
     * Simple text based vertex writer
     */
    private static class SimpleTextVertexWriter
            extends TextVertexWriter<LongWritable, IntWritable, FloatWritable> {

        /**
         * Initialize with the LineRecordWriter.
         *
         * @param lineRecordWriter Line record writer from TextOutputFormat
         */
        public SimpleTextVertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(
                BasicVertex<LongWritable, IntWritable, FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
            getRecordWriter().write(
                new Text(vertex.getVertexId().toString()),
                new Text(vertex.getVertexValue().toString()));
        }
    }

    @Override
    public VertexWriter<LongWritable, IntWritable, FloatWritable>
        createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        RecordWriter<Text, Text> recordWriter =
            textOutputFormat.getRecordWriter(context);
        return new SimpleTextVertexWriter(recordWriter);
    }
}
