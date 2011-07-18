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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.log4j.Logger;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
public class SimplePageRankVertex extends
        Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    /** User can access this sum after the application finishes if local */
    public static long finalSum;
    /** User can access this min after the application finishes if local */
    public static double finalMin;
    /** User can access this max after the application finishes if local */
    public static double finalMax;
    /** Logger */
    private static final Logger LOG =
        Logger.getLogger(SimplePageRankVertex.class);

    @Override
    public void preApplication()
            throws InstantiationException, IllegalAccessException {
        registerAggregator("sum", LongSumAggregator.class);
        registerAggregator("min", MinAggregator.class);
        registerAggregator("max", MaxAggregator.class);
    }

    @Override
    public void postApplication() {
        LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator("sum");
        MinAggregator minAggreg = (MinAggregator) getAggregator("min");
        MaxAggregator maxAggreg = (MaxAggregator) getAggregator("max");
        finalSum = sumAggreg.getAggregatedValue().get();
        finalMin = minAggreg.getAggregatedValue().get();
        finalMax = maxAggreg.getAggregatedValue().get();

    }

    @Override
    public void preSuperstep() {
        LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator("sum");
        MinAggregator minAggreg = (MinAggregator) getAggregator("min");
        MaxAggregator maxAggreg = (MaxAggregator) getAggregator("max");
        if (getSuperstep() >= 3) {
            LOG.info("aggregatedNumVertices=" +
                    sumAggreg.getAggregatedValue() +
                    " NumVertices=" + getNumVertices());
            if (sumAggreg.getAggregatedValue().get() != getNumVertices()) {
                throw new RuntimeException("wrong value of SumAggreg: " +
                        sumAggreg.getAggregatedValue() + ", should be: " +
                        getNumVertices());
            }
            DoubleWritable maxPagerank =
                    (DoubleWritable)maxAggreg.getAggregatedValue();
            LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
            DoubleWritable minPagerank =
                    (DoubleWritable)minAggreg.getAggregatedValue();
            LOG.info("aggregatedMinPageRank=" + minPagerank.get());
        }
        useAggregator("sum");
        useAggregator("min");
        useAggregator("max");
        sumAggreg.setAggregatedValue(new LongWritable(0L));
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator("sum");
        MinAggregator minAggreg = (MinAggregator) getAggregator("min");
        MaxAggregator maxAggreg = (MaxAggregator) getAggregator("max");
        if (getSuperstep() > 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
            setVertexValue(vertexValue);
            maxAggreg.aggregate(vertexValue);
            minAggreg.aggregate(vertexValue);
            sumAggreg.aggregate(1L);
            LOG.info(getVertexId() + ": PageRank=" + vertexValue +
                     " max=" + maxAggreg.getAggregatedValue() +
                     " min=" + minAggreg.getAggregatedValue());
        }

        if (getSuperstep() < 30) {
            long edges = getOutEdgeMap().size();
            sentMsgToAllEdges(
                new DoubleWritable(getVertexValue().get() / edges));
        } else {
            voteToHalt();
        }
    }

    /**
     * Simple VertexReader that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexReader extends
            GeneratedVertexReader<LongWritable, DoubleWritable, FloatWritable> {
        /** Class logger */
        private static final Logger LOG =
            Logger.getLogger(SimplePageRankVertexReader.class);
        @Override
        public boolean next(MutableVertex<LongWritable, DoubleWritable,
                            FloatWritable, ?> vertex) throws IOException {
            if (totalRecords <= recordsRead) {
                return false;
            }
            vertex.setVertexId(new LongWritable(
                (inputSplit.getSplitIndex() * totalRecords) + recordsRead));
            vertex.setVertexValue(
                new DoubleWritable(vertex.getVertexId().get() * 10d));
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
     * Simple VertexInputFormat that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexInputFormat extends
            GeneratedVertexInputFormat<LongWritable,
            DoubleWritable, FloatWritable> {
        @Override
        public VertexReader<LongWritable, DoubleWritable, FloatWritable>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new SimplePageRankVertexReader();
        }
    }

    /**
     * Simple VertexWriter that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexWriter extends
            TextVertexWriter<LongWritable, DoubleWritable, FloatWritable> {
        public SimplePageRankVertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(
                BasicVertex<LongWritable, DoubleWritable, FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
            getRecordWriter().write(
                new Text(vertex.getVertexId().toString()),
                new Text(vertex.getVertexValue().toString()));
        }
    }

    /**
     * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {

        @Override
        public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
            createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new SimplePageRankVertexWriter(recordWriter);
        }
    }
}
