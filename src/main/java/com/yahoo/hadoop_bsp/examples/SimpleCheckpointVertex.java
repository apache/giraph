package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;
import java.util.Map;

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

import com.yahoo.hadoop_bsp.BspJob;
import com.yahoo.hadoop_bsp.HadoopVertex;
import com.yahoo.hadoop_bsp.OutEdgeIterator;
import com.yahoo.hadoop_bsp.VertexInputFormat;
import com.yahoo.hadoop_bsp.lib.LongSumAggregator;

/**
 * An example that simply uses its id, value, and edges to compute new data
 * every iteration to verify that checkpoint restarting works.  Fault injection
 * can also test automated checkpoint restarts.
 */
public class SimpleCheckpointVertex extends
        HadoopVertex<LongWritable, IntWritable, FloatWritable, FloatWritable>
        implements Tool {
    /** Configuration */
    private static Configuration conf;
    /** User can access this after the application finishes if local */
    public static long finalSum;
    /** Number of supersteps to run (6 by default) */
    public static int supersteps = 6;
    /** Filename to indicate whether a fault was found */
    public final String faultFile = "/tmp/faultFile";
    /** Which superstep to cause the worker to fail */
    public final int faultingSuperstep = 4;
    /** Vertex id to fault on */
    public final long faultingVertexId = 1;
    /** Enable the fault at the particular vertex id and superstep? */
    public static boolean enableFault = false;

    /** Dynamically set number of supersteps */
    public static String SUPERSTEP_COUNT =
        "simpleCheckpointVertex.superstepCount";
    /** Should fault? */
    public static String ENABLE_FAULT=
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
        System.out.println("compute: vertex " + getVertexId() + " has value " +
                           getVertexValue() + " on superstep " + getSuperstep());
        OutEdgeIterator<LongWritable, FloatWritable> it = getOutEdgeIterator();
        while (it.hasNext()) {
            Map.Entry<LongWritable, FloatWritable> entry = it.next();
            float edgeValue = entry.getValue().get();
            System.out.println("compute: vertex " + getVertexId() +
                               " sending edgeValue " + edgeValue +
                               " vertexValue " + vertexValue +
                               " total " + (edgeValue + (float) vertexValue) +
                               " to vertex " + entry.getKey() +
                               " on superstep " + getSuperstep());
            entry.getValue().set(edgeValue + (float) vertexValue);
            sendMsg(entry.getKey(), new FloatWritable(edgeValue));
        }
    }

    public FloatWritable createMsgValue() {
        return new FloatWritable(0);
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
            System.exit(0);
        }
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption('h')) {
            formatter.printHelp(getClass().getName(), options, true);
            System.exit(0);
        }
        if (!cmd.hasOption('w')) {
            System.out.println("Need to choose the number of workers (-w)");
            System.exit(-1);
        }
        if (!cmd.hasOption('o')) {
            System.out.println("Need to set the output directory (-o)");
            System.exit(-1);
        }

        getConf().setClass(BspJob.VERTEX_CLASS, getClass(), HadoopVertex.class);
        getConf().setClass(BspJob.VERTEX_INPUT_FORMAT_CLASS,
                           GeneratedVertexInputFormat.class,
                           VertexInputFormat.class);
        getConf().setInt(BspJob.MIN_WORKERS,
                         Integer.parseInt(cmd.getOptionValue('w')));
        getConf().setInt(BspJob.MAX_WORKERS,
                         Integer.parseInt(cmd.getOptionValue('w')));
        BspJob bspJob = new BspJob(getConf(), getClass().getName());
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
        SimpleCheckpointVertex.conf = conf;
    }
}
