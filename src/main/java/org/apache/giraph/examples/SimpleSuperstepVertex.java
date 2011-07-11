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
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import org.apache.giraph.examples.TextVertexOutputFormat.TextVertexWriter;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;

/**
 * Just a simple Vertex compute implementation that executes 3 supersteps, then
 * finishes.
 */
public class SimpleSuperstepVertex extends
        Vertex<LongWritable, IntWritable, FloatWritable, IntWritable> {
    @Override
    public void compute(Iterator<IntWritable> msgIterator) {
        if (getSuperstep() > 3) {
            voteToHalt();
        }
    }

    /**
     * Simple VertexReader that supports {@link SimpleSuperstepVertex}
     */
    public static class SimpleSuperstepVertexReader extends
            GeneratedVertexReader<LongWritable, IntWritable, FloatWritable> {
        /** Class logger */
        private static final Logger LOG =
            Logger.getLogger(SimpleSuperstepVertexReader.class);
        @Override
        public boolean next(MutableVertex<LongWritable, IntWritable,
                            FloatWritable, ?> vertex) throws IOException {
            if (totalRecords <= recordsRead) {
                return false;
            }
            vertex.setVertexId(new LongWritable(
                (inputSplit.getSplitIndex() * totalRecords) + recordsRead));
            vertex.setVertexValue(
                new IntWritable((int) (vertex.getVertexId().get() * 10)));
            long destVertexId =
                (vertex.getVertexId().get() + 1) %
                (inputSplit.getNumSplits() * totalRecords);
            float edgeValue = vertex.getVertexId().get() * 100f;
            // Adds an edge to the neighbor vertex
            vertex.addEdge(new Edge<LongWritable, FloatWritable>(
                    new LongWritable(destVertexId),
                    new FloatWritable(edgeValue)));
            ++recordsRead;
            LOG.info("next: Return vertexId=" + vertex.getVertexId().get() +
                ", vertexValue=" + vertex.getVertexValue() +
                ", destinationId=" + destVertexId + ", edgeValue=" + edgeValue);
            return true;
        }
    }

    /**
     * Simple VertexInputFormat that supports {@link SimpleSuperstepVertex}
     */
    public static class SimpleSuperstepVertexInputFormat extends
            GeneratedVertexInputFormat<LongWritable,
            IntWritable, FloatWritable> {
        @Override
        public VertexReader<LongWritable, IntWritable, FloatWritable>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new SimpleSuperstepVertexReader();
        }
    }

    /**
     * Simple VertexWriter that supports {@link SimpleSuperstepVertex}
     */
    public static class SimpleSuperstepVertexWriter extends
            TextVertexWriter<LongWritable, IntWritable, FloatWritable> {
        public SimpleSuperstepVertexWriter(
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

    /**
     * Simple VertexOutputFormat that supports {@link SimpleSuperstepVertex}
     */
    public static class SimpleSuperstepVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, IntWritable, FloatWritable> {

        @Override
        public VertexWriter<LongWritable, IntWritable, FloatWritable>
            createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new SimpleSuperstepVertexWriter(recordWriter);
        }
    }
}
