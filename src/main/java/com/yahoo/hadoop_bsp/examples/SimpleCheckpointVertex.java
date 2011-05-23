package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
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
 * every iteration to verify that checkpoint restarting works.
 *
 * @author aching
 */
public class SimpleCheckpointVertex extends
        HadoopVertex<LongWritable, IntWritable, FloatWritable, FloatWritable>
        implements Tool {
    /** User can access this after the application finishes if local */
    public static long finalSum;

    @Override
    public void preApplication() {
        registerAggregator(LongSumAggregator.class.getName(),
                           LongSumAggregator.class);
        LongSumAggregator sumAggregator = (LongSumAggregator)
            getAggregator(LongSumAggregator.class.getName());
        sumAggregator.setAggregatedValue(new LongWritable(0));
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
        if (getSuperstep() > 6) {
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
        options.addOption("w",
                          "workers",
                          true,
                          "Minimum number of workers");
        options.addOption("o",
                          "output directory",
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
        if (bspJob.run(verbose) == true) {
            return 0;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SimpleCheckpointVertex(), args));
    }
}
