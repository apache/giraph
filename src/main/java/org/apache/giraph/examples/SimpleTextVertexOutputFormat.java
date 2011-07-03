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

package org.apache.giraph.examples;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;

/**
 * Simple text based vertex output format example .
 */
public class SimpleTextVertexOutputFormat extends
         TextVertexOutputFormat<LongWritable, IntWritable, FloatWritable> {
    /**
     * Simple text based vertex writer
     */
    private static class SimpleTextVertexWriter
            implements VertexWriter<LongWritable, IntWritable, FloatWritable> {
        /** Internal line record reader */
        private final LineRecordWriter<Text, Text> lineRecordWriter;

        /**
         * Constructor.
         *
         * @param outputStream Where to write
         * @param keyValueSeparator separator to use between key and value
         */
        public SimpleTextVertexWriter(DataOutputStream outputStream,
                                      String keyValueSeparator) {
            lineRecordWriter =
                new LineRecordWriter<Text, Text>(outputStream,
                                                 keyValueSeparator);
        }

        @Override
        public void initialize(TaskAttemptContext context) throws IOException {
        }

        @Override
        public void writeVertex(
                BasicVertex<LongWritable, IntWritable, FloatWritable, ?> vertex)
                throws IOException {
            lineRecordWriter.write(
                new Text(vertex.getVertexId().toString()),
                new Text(vertex.getVertexValue().toString()));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            lineRecordWriter.close(context);
        }
    }

    @Override
    public VertexWriter<LongWritable, IntWritable, FloatWritable>
            createVertexWriter(TaskAttemptContext context) throws IOException {
        // Note: This code is basically code from TextOutputFormat
        // getRecordWriter().  Copy is required to get the vertex writer with
        // the right construction arguments.
        Configuration conf = context.getConfiguration();
        boolean isCompressed = getCompressOutput(context);
        String keyValueSeparator =
            conf.get("mapred.textoutputformat.separator", "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass =
                getOutputCompressorClass(context, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
                                                                   conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(context, extension);
        FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new SimpleTextVertexWriter(fileOut, keyValueSeparator);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new SimpleTextVertexWriter(new DataOutputStream
                (codec.createOutputStream(fileOut)),
                keyValueSeparator);
        }
    }
}
