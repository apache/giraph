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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class SimpleShortestPathsVertex extends
        Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
        implements Tool {
    /** Configuration */
    private Configuration conf;
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(SimpleShortestPathsVertex.class);
    /** The shortest paths id */
    public static String SOURCE_ID = "SimpleShortestPathsVertex.sourceId";
    /** Default shortest paths id */
    public static long SOURCE_ID_DEFAULT = 1;

    /**
     * Is this vertex the source id?
     *
     * @return True if the source id
     */
    private boolean isSource() {
        return (getVertexId().get() ==
            getContext().getConfiguration().getLong(SOURCE_ID,
                                                    SOURCE_ID_DEFAULT));
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (getSuperstep() == 0) {
            setVertexValue(new DoubleWritable(Double.MAX_VALUE));
        }
        double minDist = isSource() ? 0d : Double.MAX_VALUE;
        while (msgIterator.hasNext()) {
            minDist = Math.min(minDist, msgIterator.next().get());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + getVertexId() + " got minDist = " + minDist +
                     " vertex value = " + getVertexValue());
        }
        if (minDist < getVertexValue().get()) {
            setVertexValue(new DoubleWritable(minDist));
            for (Edge<LongWritable, FloatWritable> edge :
                    getOutEdgeMap().values()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Vertex " + getVertexId() + " sent to " +
                              edge.getDestVertexId() + " = " +
                              (minDist + edge.getEdgeValue().get()));
                }
                sendMsg(edge.getDestVertexId(),
                        new DoubleWritable(minDist +
                                           edge.getEdgeValue().get()));
            }
        }
        voteToHalt();
    }

    /**
     * VertexInputFormat that supports {@link SimpleShortestPathsVertex}
     */
    public static class SimpleShortestPathsVertexInputFormat extends
            TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {
        @Override
        public VertexReader<LongWritable, DoubleWritable, FloatWritable>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new SimpleShortestPathsVertexReader(
                textInputFormat.createRecordReader(split, context));
        }
    }

    /**
     * VertexReader that supports {@link SimpleShortestPathsVertex}.  In this
     * case, the edge values are not used.  The files should be in the
     * following JSON format:
     * JSONArray(<vertex id>, <vertex value>,
     *           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
     * Here is an example with vertex id 1, vertex value 4.3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,4.3,[[2,2.1],[3,0.7]]]
     */
    public static class SimpleShortestPathsVertexReader extends
            TextVertexReader<LongWritable, DoubleWritable, FloatWritable> {

        public SimpleShortestPathsVertexReader(
                RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        @Override
        public boolean next(MutableVertex<LongWritable,
                            DoubleWritable, FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
            if (!getRecordReader().nextKeyValue()) {
                return false;
            }

            Text line = getRecordReader().getCurrentValue();
            try {
                JSONArray jsonVertex = new JSONArray(line.toString());
                vertex.setVertexId(
                    new LongWritable(jsonVertex.getLong(0)));
                vertex.setVertexValue(
                    new DoubleWritable(jsonVertex.getDouble(1)));
                JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
                for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                    JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                    Edge<LongWritable, FloatWritable> edge =
                        new Edge<LongWritable, FloatWritable>(
                            new LongWritable(jsonEdge.getLong(0)),
                            new FloatWritable((float) jsonEdge.getDouble(1)));
                    vertex.addEdge(edge);
                }
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Couldn't get vertex from line " + line, e);
            }
            return true;
        }
    }

    /**
     * VertexOutputFormat that supports {@link SimpleShortestPathsVertex}
     */
    public static class SimpleShortestPathsVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, DoubleWritable,
            FloatWritable> {

        @Override
        public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
                createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new SimpleShortestPathsVertexWriter(recordWriter);
        }
    }

    /**
     * VertexWriter that supports {@link SimpleShortestPathsVertex}
     */
    public static class SimpleShortestPathsVertexWriter extends
            TextVertexWriter<LongWritable, DoubleWritable, FloatWritable> {
        public SimpleShortestPathsVertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<LongWritable, DoubleWritable,
                                FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
            JSONArray jsonVertex = new JSONArray();
            try {
                jsonVertex.put(vertex.getVertexId().get());
                jsonVertex.put(vertex.getVertexValue().get());
                JSONArray jsonEdgeArray = new JSONArray();
                for (Edge<LongWritable, FloatWritable> edge :
                        vertex.getOutEdgeMap().values()) {
                    JSONArray jsonEdge = new JSONArray();
                    jsonEdge.put(edge.getDestVertexId().get());
                    jsonEdge.put(edge.getEdgeValue().get());
                    jsonEdgeArray.put(jsonEdge);
                }
                jsonVertex.put(jsonEdgeArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "writeVertex: Couldn't write vertex " + vertex);
            }
            getRecordWriter().write(new Text(jsonVertex.toString()), null);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] argArray) throws Exception {
        if (argArray.length != 4) {
            throw new IllegalArgumentException(
                "run: Must have 4 arguments <input path> <output path> " +
                "<source vertex id> <# of workers>");
        }
        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.setVertexClass(getClass());
        job.setVertexInputFormatClass(
            SimpleShortestPathsVertexInputFormat.class);
        job.setVertexOutputFormatClass(
            SimpleShortestPathsVertexOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(argArray[0]));
        FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
        job.getConfiguration().setLong(SimpleShortestPathsVertex.SOURCE_ID,
                                       Long.parseLong(argArray[2]));
        job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
                                   Integer.parseInt(argArray[3]),
                                   100.0f);
        if (job.run(true) == true) {
            return 0;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SimpleShortestPathsVertex(), args));
    }
}
