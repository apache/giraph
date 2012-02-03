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
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.HashMapVertex;
import org.apache.giraph.graph.MutableVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Iterator;

/**
 * Benchmark based on the basic Pregel PageRank implementation.
 */
public class PageRankBenchmark implements Tool {
    /** Configuration from Configurable */
    private Configuration conf;

    /** How many supersteps to run */
    public static String SUPERSTEP_COUNT = "PageRankBenchmark.superstepCount";

    private static void computePageRank(
            MutableVertex
              <LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>
                vertex,
            Iterator<DoubleWritable> msgIterator) {
        if (vertex.getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / vertex.getNumVertices()) + 0.85f *
                                   sum);
            vertex.setVertexValue(vertexValue);
        }

        if (vertex.getSuperstep() < vertex.getConf().getInt(SUPERSTEP_COUNT, -1)) {
            long edges = vertex.getNumOutEdges();
            vertex.sendMsgToAllEdges(
                new DoubleWritable(vertex.getVertexValue().get() / edges));
        } else {
            vertex.voteToHalt();
        }
    }

    public static class PageRankHashMapVertex extends HashMapVertex<
            LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
        @Override
        public void compute(Iterator<DoubleWritable> msgIterator) {
            PageRankBenchmark.computePageRank(this, msgIterator);
        }
    }

    public static class PageRankEdgeListVertex extends EdgeListVertex<
            LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
        @Override
        public void compute(Iterator<DoubleWritable> msgIterator) {
            PageRankBenchmark.computePageRank(this, msgIterator);
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
        options.addOption("c",
                          "vertexClass",
                          true,
                          "Vertex class (0 for Vertex, 1 for EdgeListVertex)");
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
            System.out.println("Need to set the number of supersteps (-s)");
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

        int workers = Integer.parseInt(cmd.getOptionValue('w'));
        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        if (!cmd.hasOption('c') ||
                (Integer.parseInt(cmd.getOptionValue('c')) == 0)) {
            System.out.println("Using " +
                                PageRankHashMapVertex.class.getName());
            job.setVertexClass(PageRankHashMapVertex.class);
        } else {
            System.out.println("Using " +
                                PageRankEdgeListVertex.class.getName());
            job.setVertexClass(PageRankEdgeListVertex.class);
        }
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

        boolean isVerbose = false;
        if (cmd.hasOption('v')) {
            isVerbose = true;
        }
        if (cmd.hasOption('s')) {
            getConf().setInt(SUPERSTEP_COUNT,
                             Integer.parseInt(cmd.getOptionValue('s')));
        }
        if (job.run(isVerbose) == true) {
            return 0;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PageRankBenchmark(), args));
    }
}
