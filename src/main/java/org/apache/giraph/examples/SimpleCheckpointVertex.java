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

import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexOutputFormat;

/**
 * An example that simply uses its id, value, and edges to compute new data
 * every iteration to verify that checkpoint restarting works.  Fault injection
 * can also test automated checkpoint restarts.
 */
public class SimpleCheckpointVertex extends
        Vertex<LongWritable, IntWritable, FloatWritable, FloatWritable>
        implements Tool {
    /** Configuration */
    private Configuration conf;
    /** User can access this after the application finishes if local */
    public static long finalSum;
    /** Number of supersteps to run (6 by default) */
    private static int supersteps = 6;
    /** Filename to indicate whether a fault was found */
    public final String faultFile = "/tmp/faultFile";
    /** Which superstep to cause the worker to fail */
    public final int faultingSuperstep = 4;
    /** Vertex id to fault on */
    public final long faultingVertexId = 1;
    /** Enable the fault at the particular vertex id and superstep? */
    private static boolean enableFault = false;

    /** Dynamically set number of supersteps */
    public static final String SUPERSTEP_COUNT =
        "simpleCheckpointVertex.superstepCount";
    /** Should fault? */
    public static final String ENABLE_FAULT=
        "simpleCheckpointVertex.enableFault";

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {
        registerAggregator(LongSumAggregator.class.getName(),
                           LongSumAggregator.class);
        LongSumAggregator sumAggregator = (LongSumAggregator)
            getAggregator(LongSumAggregator.class.getName());
        sumAggregator.setAggregatedValue(new LongWritable(0));
        supersteps = getConf().getInt(SUPERSTEP_COUNT, supersteps);
        enableFault = getConf().getBoolean(ENABLE_FAULT, false);
    }

    @Override
    public void postApplication() {
        LongSumAggregator sumAggregator = (LongSumAggregator)
            getAggregator(LongSumAggregator.class.getName());
        finalSum = sumAggregator.getAggregatedValue().get();
    }

    @Override
    public void preSuperstep() {
        useAggregator(LongSumAggregator.class.getName());
    }

    public void compute(Iterator<FloatWritable> msgIterator) {
        LongSumAggregator sumAggregator = (LongSumAggregator)
            getAggregator(LongSumAggregator.class.getName());
        if (enableFault && (getSuperstep() == faultingSuperstep) &&
                (getContext().getTaskAttemptID().getId() == 0) &&
                (getVertexId().get() == faultingVertexId)) {
            System.out.println("compute: Forced a fault on the first " +
                               "attempt of superstep " +
                               faultingSuperstep + " and vertex id " +
                               faultingVertexId);
            System.exit(-1);
        }
        if (getSuperstep() > supersteps) {
            voteToHalt();
        }
        System.out.println("compute: " + sumAggregator);
        sumAggregator.aggregate(getVertexId().get());
        System.out.println("compute: sum = " +
                           sumAggregator.getAggregatedValue().get() +
                           " for vertex " + getVertexId());
        float msgValue = 0.0f;
        while (msgIterator.hasNext()) {
            float curMsgValue = msgIterator.next().get();
            msgValue += curMsgValue;
            System.out.println("compute: got msgValue = " + curMsgValue +
                               " for vertex " + getVertexId() +
                               " on superstep " + getSuperstep());
        }
        int vertexValue = getVertexValue().get();
        setVertexValue(new IntWritable(vertexValue + (int) msgValue));
        System.out.println("compute: vertex " + getVertexId() +
                           " has value " + getVertexValue() +
                           " on superstep " + getSuperstep());
        for (Edge<LongWritable, FloatWritable> edge : getOutEdgeMap().values()) {
            float edgeValue = edge.getEdgeValue().get();
            System.out.println("compute: vertex " + getVertexId() +
                               " sending edgeValue " + edgeValue +
                               " vertexValue " + vertexValue +
                               " total " + (edgeValue + (float) vertexValue) +
                               " to vertex " + edge.getDestinationVertexIndex() +
                               " on superstep " + getSuperstep());
            edge.getEdgeValue().set(edgeValue + (float) vertexValue);
            sendMsg(edge.getDestinationVertexIndex(),
                    new FloatWritable(edgeValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "Help");
        options.addOption("v", "verbose", false, "Verbose");
        options.addOption("w",
                          "workers",
                          true,
                          "Number of workers");
        options.addOption("s",
                          "supersteps",
                          true,
                          "Supersteps to execute before finishing");
        options.addOption("w",
                          "workers",
                          true,
                          "Minimum number of workers");
        options.addOption("o",
                          "outputDirectory",
                          true,
                          "Output directory");
        HelpFormatter formatter = new HelpFormatter();
        if (args.length == 0) {
            formatter.printHelp(getClass().getName(), options, true);
            return 0;
        }
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption('h')) {
            formatter.printHelp(getClass().getName(), options, true);
            return 0;
        }
        if (!cmd.hasOption('w')) {
            System.out.println("Need to choose the number of workers (-w)");
            return -1;
        }
        if (!cmd.hasOption('o')) {
            System.out.println("Need to set the output directory (-o)");
            return -1;
        }

        getConf().setClass(GiraphJob.VERTEX_CLASS, getClass(), Vertex.class);
        getConf().setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                           GeneratedVertexInputFormat.class,
                           VertexInputFormat.class);
        getConf().setClass(GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS,
                           SimpleTextVertexOutputFormat.class,
                           VertexOutputFormat.class);
        getConf().setInt(GiraphJob.MIN_WORKERS,
                         Integer.parseInt(cmd.getOptionValue('w')));
        getConf().setInt(GiraphJob.MAX_WORKERS,
                         Integer.parseInt(cmd.getOptionValue('w')));
        GiraphJob bspJob = new GiraphJob(getConf(), getClass().getName());
        FileOutputFormat.setOutputPath(bspJob,
                                       new Path(cmd.getOptionValue('o')));
        boolean verbose = false;
        if (cmd.hasOption('v')) {
            verbose = true;
        }
        if (cmd.hasOption('s')) {
            getConf().setInt(SUPERSTEP_COUNT,
                             Integer.parseInt(cmd.getOptionValue('s')));
        }
        if (bspJob.run(verbose) == true) {
            return 0;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SimpleCheckpointVertex(), args));
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
