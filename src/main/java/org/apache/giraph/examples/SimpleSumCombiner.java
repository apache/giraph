package org.apache.giraph.examples;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.giraph.comm.CommunicationsInterface;
import org.apache.giraph.graph.Combiner;

/**
 * Test whether combiner is called by summing up the messages.
 */
public class SimpleSumCombiner
        implements Combiner<LongWritable, IntWritable,
        FloatWritable, IntWritable> {

    @Override
    public void combine(
            CommunicationsInterface<LongWritable, IntWritable,
                                    FloatWritable, IntWritable> comm,
            LongWritable vertex, List<IntWritable> msgList)
            throws IOException {
        int sum = 0;
        for (IntWritable msg : msgList) {
            sum += msg.get();
        }
        comm.putMsg(vertex, new IntWritable(sum));
    }
}
