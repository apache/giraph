/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Iterator;
import java.util.Random;

/**
 * Random Message Benchmark for evaluating the message delivery feature
 */
public class RandomMessageBenchmark extends
        Vertex<LongWritable, DoubleWritable, DoubleWritable, BytesWritable>
        implements Tool {
    /** Configuration from Configurable */
    private Configuration conf;

    /** How many supersteps to run */
    public static String SUPERSTEP_COUNT =
        "RandomMessageBenchmark.superstepCount";

    /** How many bytes per message */
    public static String NUM_BYTES_PER_MESSAGE =
        "RandomMessageBenchmark.numBytesPerMessage";
    /** How many bytes per message */
    public static String NUM_MESSAGES_PER_VERTEX =
        "RandomMessageBenchmark.numMessagesPerVertex";

    /** Random generator for random bytes message */
    private Random rnd = new Random(System.currentTimeMillis());

    @Override
    public void compute(Iterator<BytesWritable> msgIterator) {
        byte [] message = new byte[getConf().getInt(NUM_BYTES_PER_MESSAGE, 16)];
        int numMessage = getConf().getInt(NUM_MESSAGES_PER_VERTEX, 1);
        if (getSuperstep() < getConf().getInt(SUPERSTEP_COUNT, -1)) {
            for (int i=0; i < numMessage; i++) {
                rnd.nextBytes(message);
                sendMsgToAllEdges(new BytesWritable(message));
            }
        } else {
            voteToHalt();
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
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "Help");
        options.addOption("v", "verbose", false, "Verbose");
        options.addOption("w",
                "workers",
                true,
                "Number of workers");
        options.addOption("b",
                "bytes",
                true,
                "Message bytes per memssage");
        options.addOption("n",
                "number",
                true,
                "Number of messages per edge");
        options.addOption("s",
                "supersteps",
                true,
                "Supersteps to execute before finishing");
        options.addOption("V",
                "aggregateVertices",
                true,
                "Aggregate vertices");
        options.addOption("e",
                "edgesPerVertex",
                true,
                "Edges per vertex");
        options.addOption("f",
                "flusher",
                true,
                "Number of flush threads");

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
        if (!cmd.hasOption('s')) {
            System.out.println("Need to set the number of supesteps (-s)");
            return -1;
        }
        if (!cmd.hasOption('V')) {
            System.out.println("Need to set the aggregate vertices (-V)");
            return -1;
        }
        if (!cmd.hasOption('e')) {
            System.out.println("Need to set the number of edges " +
                               "per vertex (-e)");
            return -1;
        }
        if (!cmd.hasOption('b')) {
            System.out.println("Need to set the number of message bytes (-b)");
            return -1;
        }
        if (!cmd.hasOption('n')) {
            System.out.println("Need to set the number of messages per edge (-n)");
            return -1;
        }
        int workers = Integer.parseInt(cmd.getOptionValue('w'));
        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
        job.setVertexClass(getClass());
        job.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
        job.setWorkerConfiguration(workers, workers, 100.0f);
        job.getConfiguration().setLong(
            PseudoRandomVertexInputFormat.AGGREGATE_VERTICES,
            Long.parseLong(cmd.getOptionValue('V')));
        job.getConfiguration().setLong(
            PseudoRandomVertexInputFormat.EDGES_PER_VERTEX,
            Long.parseLong(cmd.getOptionValue('e')));
        job.getConfiguration().setInt(
            SUPERSTEP_COUNT,
            Integer.parseInt(cmd.getOptionValue('s')));
        job.getConfiguration().setInt(
            RandomMessageBenchmark.NUM_BYTES_PER_MESSAGE,
            Integer.parseInt(cmd.getOptionValue('b')));
        job.getConfiguration().setInt(
            RandomMessageBenchmark.NUM_MESSAGES_PER_VERTEX,
            Integer.parseInt(cmd.getOptionValue('n')));

        boolean isVerbose = false;
        if (cmd.hasOption('v')) {
            isVerbose = true;
        }
        if (cmd.hasOption('s')) {
            getConf().setInt(SUPERSTEP_COUNT,
                             Integer.parseInt(cmd.getOptionValue('s')));
        }
        if (cmd.hasOption('f')) {
            job.getConfiguration().setInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
                Integer.parseInt(cmd.getOptionValue('f')));
        }
        if (job.run(isVerbose) == true) {
            return 0;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RandomMessageBenchmark(), args));
    }
}
