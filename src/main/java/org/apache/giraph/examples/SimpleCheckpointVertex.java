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

package org.apache.giraph.examples;

import org.apache.commons.cli.*;
import org.apache.giraph.graph.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * An example that simply uses its id, value, and edges to compute new data
 * every iteration to verify that checkpoint restarting works.  Fault injection
 * can also test automated checkpoint restarts.
 */
public class SimpleCheckpointVertex extends
        Vertex<LongWritable, IntWritable, FloatWritable, FloatWritable>
        implements Tool {
    private static Logger LOG =
        Logger.getLogger(SimpleCheckpointVertex.class);
    /** Configuration */
    private Configuration conf;
    /** Which superstep to cause the worker to fail */
    public final int faultingSuperstep = 4;
    /** Vertex id to fault on */
    public final long faultingVertexId = 1;
    /** Dynamically set number of supersteps */
    public static final String SUPERSTEP_COUNT =
        "simpleCheckpointVertex.superstepCount";
    /** Should fault? */
    public static final String ENABLE_FAULT=
        "simpleCheckpointVertex.enableFault";

    @Override
    public void compute(Iterator<FloatWritable> msgIterator) {
    	SimpleCheckpointVertexWorkerContext workerContext = 
    		(SimpleCheckpointVertexWorkerContext) getWorkerContext();
    	
        LongSumAggregator sumAggregator = (LongSumAggregator)
            getAggregator(LongSumAggregator.class.getName());
        
        boolean enableFault = workerContext.getEnableFault();
        int supersteps = workerContext.getSupersteps();
        
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
            return;
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
        for (LongWritable targetVertexId : this) {
            FloatWritable edgeValue = getEdgeValue(targetVertexId);
            System.out.println("compute: vertex " + getVertexId() +
                               " sending edgeValue " + edgeValue +
                               " vertexValue " + vertexValue +
                               " total " + (edgeValue.get() +
                               (float) vertexValue) +
                               " to vertex " + targetVertexId +
                               " on superstep " + getSuperstep());
            edgeValue.set(edgeValue.get() + (float) vertexValue);
            addEdge(targetVertexId, edgeValue);
            sendMsg(targetVertexId, new FloatWritable(edgeValue.get()));
        }
    }
    
    public static class SimpleCheckpointVertexWorkerContext 
            extends WorkerContext {
        /** User can access this after the application finishes if local */
        public static long finalSum;
        /** Number of supersteps to run (6 by default) */
        private int supersteps = 6;
        /** Filename to indicate whether a fault was found */
        public final String faultFile = "/tmp/faultFile";
        /** Enable the fault at the particular vertex id and superstep? */
        private boolean enableFault = false;

		@Override
		public void preApplication() 
		        throws InstantiationException, IllegalAccessException {
		    registerAggregator(LongSumAggregator.class.getName(),
					LongSumAggregator.class);
		    LongSumAggregator sumAggregator = (LongSumAggregator)
		    getAggregator(LongSumAggregator.class.getName());
		    sumAggregator.setAggregatedValue(new LongWritable(0));
		    supersteps = getContext().getConfiguration()
		        .getInt(SUPERSTEP_COUNT, supersteps);
		    enableFault = getContext().getConfiguration()
		        .getBoolean(ENABLE_FAULT, false);
		}

		@Override
		public void postApplication() {
		    LongSumAggregator sumAggregator = (LongSumAggregator)
		        getAggregator(LongSumAggregator.class.getName());
		    finalSum = sumAggregator.getAggregatedValue().get();
		    LOG.info("finalSum="+ finalSum);
		}

		@Override
		public void preSuperstep() {
	        useAggregator(LongSumAggregator.class.getName());
		}

		@Override
		public void postSuperstep() { }
		
		public int getSupersteps() {
		    return this.supersteps;
		}

		public boolean getEnableFault() {
		    return this.enableFault;
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

        GiraphJob bspJob = new GiraphJob(getConf(), getClass().getName());
        bspJob.setVertexClass(getClass());
        bspJob.setVertexInputFormatClass(GeneratedVertexInputFormat.class);
        bspJob.setVertexOutputFormatClass(SimpleTextVertexOutputFormat.class);
        bspJob.setWorkerContextClass(SimpleCheckpointVertexWorkerContext.class);
        int minWorkers = Integer.parseInt(cmd.getOptionValue('w'));
        int maxWorkers = Integer.parseInt(cmd.getOptionValue('w'));
        bspJob.setWorkerConfiguration(minWorkers, maxWorkers, 100.0f);
        
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
